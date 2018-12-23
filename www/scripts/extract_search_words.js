const shm = require('shm-typed-array')
const fs = require('fs')

const utils = require('./utils')
const { mkdirIfExistsSync, unlinkIfNotExistsSync } = utils
const { wordsIndexDeserialize, wordsIndexSerialize, writeWordsIndex } = utils
const { startClusteredDBScanning, cleanupDescription, sleep } = utils
const { splitTextToLexemes } = require('./utils/common')

// TODO: 12.2007г, 2007.12.01 и т.д.
// TODO: https://ru.wikipedia.org/wiki/Мнемоники_в_HTML

mkdirIfExistsSync('gen')
unlinkIfNotExistsSync('gen/lockfile')

startClusteredDBScanning({
	fields: 'id, title, description',
	//  ============
	// === МАСТЕР ===
	//  ============
	masterSetUp: function(cluster, startWorkers) {
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
				let tps = Math.round((torrentsCount / (Date.now() - startStamp)) * 1000)
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

		startWorkers({
			extraParams: {
				sharedBufKey: sharedBuf.key,
			},
		})
	},
	//  ============
	// === ВОРКЕР ===
	//  ============
	workerSetUp: function(extraParams) {
		let torrentsCount = 0
		let sharedBuf = shm.get(extraParams.sharedBufKey, 'Buffer')
		let wordsIndex = new Map()
		let wordsIndexTitle = new Map()

		function processText(wordsIndex, torrentID, text) {
			splitTextToLexemes(text).forEach(lexeme => {
				let val = wordsIndex.get(lexeme)
				if (val === undefined) {
					wordsIndex.set(lexeme, new Set([torrentID]))
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

			// отправка насчитанного мастеру
			if (torrentsCount % 5000 == 0) {
				// блокировка: пытаемся создать и открыть файл тогда, когда его ещё нет
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

		return { handleRow }
	},
})

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
