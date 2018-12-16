const sqlite3 = require('sqlite3').verbose()
const cluster = require('cluster')
const shm = require('shm-typed-array')
const fs = require('fs')

const utils = require('./utils')
const { mkdirIfExistsSync, unlinkIfNotExistsSync, processIsRunning } = utils
const { wordsIndexDeserialize, wordsIndexSerialize, writeWordsIndex } = utils
const { cleanupDescription, sleep } = utils
const { splitTextToLexemes } = require('./utils/common')

// TODO: 12.2007г, 2007.12.01 и т.д.
// TODO: https://ru.wikipedia.org/wiki/Мнемоники_в_HTML

// let t = new Map()
// t.set('qwe', new Set([1, 2, 3]))
// t.set('asd', new Set([1, 2, 3, 4, 5]))
// let b = Buffer.allocUnsafe(wordsIndexBytesSize(t))
// let p = wordsIndexSerialize(b, 0, t)
// console.log(t, p, b.length, b)
// t = new Map()
// let r = wordsIndexDeserialize(b, 0, t)
// console.log(b, p, r, t)
// process.exit(1)

mkdirIfExistsSync('gen')
unlinkIfNotExistsSync('gen/lockfile')

if (cluster.isMaster) {
	//  ============
	// === МАСТЕР ===
	//  ============
	let numCPUs = require('os').cpus().length
	for (let i = 0; i < numCPUs; i++) cluster.fork()

	let torrentsCount = 0
	let startStamp = Date.now()
	let wordsIndex = new Map()
	let wordsIndexTitle = new Map()
	// буфер для быстрого получения данных от воркеров
	// (евенты тормозные до безобразия, в том чиле на больших объёмах)
	let sharedBuf = shm.create(32 * 1024 * 1024, 'Buffer')

	// сбор результатов
	cluster.on('message', (worker, msg, handle) => {
		if (msg.cmd == 'info') {
			torrentsCount += msg.torrentsCountInc
			let wordsCountBefore = wordsIndex.size

			let pos = 0
			pos = wordsIndexDeserialize(sharedBuf, pos, wordsIndexTitle)
			pos = wordsIndexDeserialize(sharedBuf, pos, wordsIndex)
			fs.unlinkSync('gen/lockfile')

			let wordsCount = wordsIndex.size
			let wordsIncK = Math.floor((wordsCount - wordsCountBefore) / 1000)
			let bufFillM = Math.floor(pos / 1024 / 1024)
			let wpt = (wordsCount / torrentsCount).toFixed(2)
			let tps = Math.round(
				(torrentsCount / (Date.now() - startStamp)) * 1000,
			)
			let logMsg = `torrents: ${torrentsCount}  words += ${wordsIncK}k = ${wordsCount}  IPC buf usage: ${bufFillM} MiB  words per torrent: ${wpt}  torrents per second: ${tps}`
			console.log(logMsg)
		}
	})

	// сохранение насчитанного
	cluster.on('exit', (worker, code, signal) => {
		if (Object.keys(cluster.workers).length == 0) {
			writeWordsIndex(wordsIndexTitle, 'title')
			writeWordsIndex(wordsIndex, 'all')
		}
	})

	// передача насальных параметров и запуск
	let i = 0
	for (const id in cluster.workers)
		cluster.workers[id].send({
			cmd: 'start',
			workerNumber: i++,
			workersCount: Object.keys(cluster.workers).length,
			sharedBufKey: sharedBuf.key,
			masterPid: process.pid,
		})
} else {
	//  ============
	// === ВОРКЕР ===
	//  ============
	let db = new sqlite3.Database('../db3/rutracker.db', sqlite3.OPEN_READONLY)
	let step = 1000
	let workersCount = null
	let workerNumber = null
	let torrentsCount = 0

	let sharedBuf = null
	let masterPid = null

	let wordsIndex = new Map()
	let wordsIndexTitle = new Map()

	function processText(wordsIndex, torrentID, text) {
		let lexemes = splitTextToLexemes(text)
		lexemes.forEach(l => {
			let val = wordsIndex.get(l)
			if (val === undefined) {
				wordsIndex.set(l, new Set([torrentID]))
			} else {
				val.add(torrentID)
			}
		})
	}
	//processText('(qwe asd) zxc "123 456" o\'rty 1-23 [1998, Франция, Криминальная комедия, DVDRip] well...well...wel --- -q-')

	let prevTorrentsCount = 0
	async function handleRow(row) {
		processText(wordsIndexTitle, row.id, row.title)
		if (row.description !== null) {
			let descr = cleanupDescription(row.description.toString())
			processText(wordsIndex, row.id, descr)
			processText(wordsIndex, row.id, row.title)
		}

		torrentsCount++
		if (torrentsCount % 5000 == 0) {
			// пытаемся создать и открыть файл тогда, когда его ещё нет
			while (true) {
				try {
					let fd = fs.openSync('gen/lockfile', 'wx')
					fs.closeSync(fd)
					break
				} catch (ex) {
					if (ex.code != 'EEXIST') throw ex
				}
				await sleep(1)
			}

			let pos = 0
			pos = wordsIndexSerialize(sharedBuf, pos, wordsIndexTitle)
			pos = wordsIndexSerialize(sharedBuf, pos, wordsIndex)

			wordsIndexTitle.clear()
			wordsIndex.clear()

			process.send({
				cmd: 'info',
				torrentsCountInc: torrentsCount - prevTorrentsCount,
			})
			prevTorrentsCount = torrentsCount
		}
	}

	function loadChunk(fromID, limit, callback) {
		let lastID = null
		db.each(
			'SELECT id, title, description, source FROM torrents WHERE id > ? AND id % ? = ? AND source like "%.xml" ORDER BY id LIMIT ?',
			[fromID, workersCount, workerNumber, limit],
			function callback(err, row) {
				if (err !== null) throw err
				lastID = row.id
				handleRow(row).catch(err => {
					throw err
				})
			},
			function complete(err) {
				if (err !== null) throw err
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

	process.on('message', function(msg) {
		if (msg.cmd == 'start') {
			workersCount = msg.workersCount
			workerNumber = msg.workerNumber
			sharedBuf = shm.get(msg.sharedBufKey, 'Buffer')
			masterPid = msg.masterPid
			console.log(`worker #${workerNumber}: started`)
			loadChunk(0, step, onChunkDone)
		}
	})
}

/*
20161015.xml
20161212.xml
20170208.xml
20141217.csv
20150204.csv
20141023.csv
20141126.csv
20150306.csv
20160115.csv
20150531.csv
20140915.csv
20151030.csv
20150409.csv
20150708.csv
final.txt
20150927.csv
20150105.csv
20151129.csv
*/
