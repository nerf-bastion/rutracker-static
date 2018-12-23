const fs = require('fs')
const cluster = require('cluster')
const sqlite3 = require('sqlite3').verbose()

//  ========================
// === ОЧИСТКА И СТЕММИНГ ===
//  ========================

const KEEP_NON_IMG_URLS = false

let _tags = 'u i b s color font spoiler align url size quote list code box pre'
let contentBBTags = new Set(_tags.split(' '))
_tags = 'b u h3 li ul a div span var pre table tr td path name disabled array dict key string'
let contentHTMLTags = new Set(_tags.split(' '))

function cleanupDescription(descr) {
	descr = unescapeHTMLCodes(descr)

	// вырезаение BB-кодов
	descr = descr.replace(/\[[hb]r\]/g, '')
	// они бывают вложенные; вырезаем, пока не кончатся
	for (let found = true; found; ) {
		found = false
		descr = descr.replace(
			/\[(\w+?)(=.*?)?\]([\s\S]*?)\[\/\1\]/g,
			(all, name, value, content) => {
				found = true
				name = name.toLowerCase()
				if (KEEP_NON_IMG_URLS && name == 'url') return value + ' ' + content
				if (name == 'img') return ''
				if (contentBBTags.has(name)) return content
				return name + ' ' + (value || '') + ' ' + content
			},
		)
	}

	// вырезаение HTML-тегов
	descr = descr.replace(/<[hb]r\s*\/>/g, '')
	for (let found = true; found; ) {
		found = false
		descr = descr.replace(
			/<(\w+?)(\s+[^>]+)?>([\s\S]*?)<\/\1>/g,
			(all, name, attrs, content) => {
				found = true
				name = name.toLowerCase()
				if (name == 'img') return ''
				if (KEEP_NON_IMG_URLS && name == 'a') {
					let m = attrs.match(/href="(.*?)"/)
					return (m ? m[1] : '') + ' ' + content
				}
				if (contentHTMLTags.has(name)) return content
				return name + ' ' + (attrs || '') + ' ' + content
			},
		)
	}

	//вырезаение ссылок на смайлики
	descr = descr.replace(/https?:\/\/static\.rutracker\.org\/smiles\/\S*/g, '')
	//вырезаение ссылок на imageshack (никто же не будет искать торрент по адресу картинки?)
	descr = descr.replace(/https?:\/\/img\d+\.imageshack.us\S*/g, '')
	//вырезаение ссылок на tinypic
	descr = descr.replace(/https?:\/\/i\d+\.tinypic\.com\/\w+\.jpe?g/g, '')
	// какие-то страныне ссылки типа http://i3.fastpic.ru/big/2009/1023/d0/49cd692e914b8...5565eee022d0.png
	descr = descr.replace(/https?:\/\/i\d+\.fastpic\.ru\/big\/[\/\w]+\.\.\.\w+\.(png|jpe?g)/g, '')
	// ipicture.ru
	descr = descr.replace(/https?:\/\/ipicture\.ru\/uploads\/[\/\w]+\.(png|jpe?g)/g, '')
	// k.foto.radikal.ru
	descr = descr.replace(/https?:\/\/k\.foto\.radikal\.ru\/[\/\w]+\.(png|jpe?g)/g, '')

	// let m = descr.match(/https?:\/\/\S+/g)
	// if (m) console.log(m.join('\n'))
	return descr
}

const mnemonicCodes = {
	amp: '&',
	quot: '"',
	lt: '<',
	gt: '>',
	ndash: '–',
	apos: "'",
}
function unescapeHTMLCodes(text) {
	return text
		.replace(/&(\w+);/g, (m, name) => mnemonicCodes[name] || m)
		.replace(/&(\w+);/g, (m, name) => mnemonicCodes[name] || m) //бывает &amp;quot;
		.replace(/&#(\d+);/g, (m, code) => String.fromCharCode(code))
}

//  ===============================
// === [ДЕ]СЕРИАЛИЗАЦИЯ ИНДЕКСОВ ===
//  ===============================

// [де]сериализация слова и айди торрентов:
// {длина слова в байтах: 4 байта} {слово в utf-8} {кол-во айдей: 4 байта} {айди: 4 байта} {айди: 4 байта} ...
function indexItemBytesSize(word, torrentIDs) {
	return 4 + Buffer.byteLength(word) + 4 + 4 * torrentIDs.size
}
function indexItemSerialize(buf, pos, word, torrentIDs) {
	pos = buf.writeInt32LE(Buffer.byteLength(word), pos)
	pos += buf.write(word, pos)
	pos = buf.writeInt32LE(torrentIDs.size, pos)
	for (let tID of torrentIDs) {
		pos = buf.writeInt32LE(tID, pos)
	}
	return pos
}
function indexItemDeserialize(buf, pos) {
	let wordBytesLen = buf.readInt32LE(pos)
	pos += 4
	let word = buf.slice(pos, pos + wordBytesLen).toString()
	pos += wordBytesLen
	let idsCount = buf.readInt32LE(pos)
	pos += 4
	let torrentIDs = new Set()
	for (let i = 0; i < idsCount; i++) {
		torrentIDs.add(buf.readInt32LE(pos))
		pos += 4
	}
	return [pos, word, torrentIDs]
}

// [де]сериализация индекса слов:
// {кол-во слов: 4 байта} {слово и список айди} {слово и список айди} {слово и список айди} ...
// про {слово и список айди} см. в indexItemSerialize
function wordsIndexBytesSize(wordsIndex) {
	let size = 4
	for (let [word, torrentIDs] of wordsIndex) size += indexItemBytesSize(word, torrentIDs)
	return size
}
function wordsIndexSerialize(buf, pos, wordsIndex) {
	pos = buf.writeInt32LE(wordsIndex.size, pos)
	for (let [word, torrentIDs] of wordsIndex) pos = indexItemSerialize(buf, pos, word, torrentIDs)
	return pos
}
function wordsIndexDeserialize(buf, pos, wordsIndex) {
	let wordsCount = buf.readInt32LE(pos)
	pos += 4
	let word, torrentIDs
	for (let i = 0; i < wordsCount; i++) {
		;[pos, word, torrentIDs] = indexItemDeserialize(buf, pos)
		let set = wordsIndex.get(word)
		if (set === undefined) {
			wordsIndex.set(word, new Set(torrentIDs))
		} else {
			for (let j = 0; j < torrentIDs.length; j++) set.add(torrentIDs[j])
		}
	}
	return pos
}

// Сериализует и сохраняет индекс слов в файл gen/words_{суффикс}
function writeWordsIndex(wordsIndex, suffix) {
	let fd = fs.openSync(`gen/words_${suffix}`, 'w')
	let buf = Buffer.allocUnsafe(wordsIndexBytesSize(wordsIndex))
	wordsIndexSerialize(buf, 0, wordsIndex)
	fs.writeSync(fd, buf)
	fs.closeSync(fd)
}

//  ================================
// === МНОГОПРОЦЕССНЫЙ ОБХОД БАЗЫ ===
//  ================================

// Запускает несколько дочерних процессов, в каждом вызывает workerSetUp,
// и потом handleRow для каждой строки из базы.
// Каждая строка попадает только в один из воркеров.
// Читает только значения столбцов fields, среди них ОБЯЗАТЕЛЬНО должен быть id.
//
// Пример:
//	startClusteredDBScanning({
//		fields: 'id, title, description',
//		masterSetUp: function(cluster, startWorkers) {
//			startWorkers({
//				extraParams: {
//					smth: 42,
//				},
//			})
//		},
//		workerSetUp: function(extraParams) {
//			let smth = extraParams.smth
//			async function handleRow(row) {
//				console.log(smth, row.id, row.title, row.description)
//			}
//			return { handleRow }
//		},
//	})
//
function startClusteredDBScanning({ fields, masterSetUp, workerSetUp }) {
	if (cluster.isMaster) {
		let numCPUs = require('os').cpus().length
		for (let i = 0; i < numCPUs; i++) cluster.fork()

		function startWorkers({ extraParams }) {
			let i = 0
			for (const id in cluster.workers)
				cluster.workers[id].send({
					cmd: 'start',
					workerNumber: i++,
					workersCount: Object.keys(cluster.workers).length,
					extraParams,
				})
		}

		masterSetUp(cluster, startWorkers)
	} else {
		let db = new sqlite3.Database('../rutracker.db', sqlite3.OPEN_READONLY)
		let step = 1000
		let workersCount = null
		let workerNumber = null
		let handleRow = null

		process.on('message', function(msg) {
			if (msg.cmd == 'start') {
				workersCount = msg.workersCount
				workerNumber = msg.workerNumber
				;({ handleRow } = workerSetUp(msg.extraParams))
				console.log(`worker #${workerNumber}: started`)
				loadChunk(0, step, onChunkDone)
			}
		})

		function loadChunk(fromID, limit, callback) {
			db.all(
				`SELECT ${fields} FROM torrents WHERE id > ? AND id % ? = ? ORDER BY id LIMIT ?`,
				[fromID, workersCount, workerNumber, limit],
				function(err, rows) {
					if (err !== null) throw err
					for (let i = 0; i < rows.length; i++)
						handleRow(rows[i]).catch(err => {
							throw err
						})
					let lastID = rows.length == 0 ? null : rows[rows.length - 1].id
					callback(lastID)
				},
			)
		}

		function onChunkDone(lastID) {
			if (lastID === null) {
				console.log(`worker #${workerNumber}: done`)
				process.exit(0)
			} else {
				loadChunk(lastID, step, onChunkDone)
			}
		}
	}
}

//  ============
// === ВСЯКОЕ ===
//  ============

function mkdirIfExistsSync(dirname) {
	try {
		fs.mkdirSync(dirname)
	} catch (ex) {
		if (ex.code != 'EEXIST') throw ex
	}
}

function unlinkIfNotExistsSync(fname) {
	try {
		fs.unlinkSync(fname)
	} catch (ex) {
		if (ex.code != 'ENOENT') throw ex
	}
}

function processIsRunning(pid) {
	try {
		process.kill(pid, 0)
	} catch (ex) {
		if (ex.code == 'ESRCH') {
			//нет процесса - будет эксепшен
			return false
		} else {
			throw ex
		}
	}
	return true
}

function sleep(mills) {
	return new Promise((res, rej) => {
		setTimeout(res, mills)
	})
}

module.exports = {
	cleanupDescription,
	unescapeHTMLCodes,
	indexItemBytesSize,
	indexItemSerialize,
	indexItemDeserialize,
	wordsIndexSerialize,
	wordsIndexDeserialize,
	writeWordsIndex,
	startClusteredDBScanning,
	mkdirIfExistsSync,
	unlinkIfNotExistsSync,
	processIsRunning,
	sleep,
}
