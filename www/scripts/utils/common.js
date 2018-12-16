const Snowball = require('snowball')

const enStemmer = new Snowball('English')
const ruStemmer = new Snowball('Russian')

function stemWord(word) {
	ruStemmer.setCurrent(word)
	ruStemmer.stem()
	let res = ruStemmer.getCurrent()
	if (res != word) return res
	enStemmer.setCurrent(word)
	enStemmer.stem()
	return enStemmer.getCurrent()
}

let trimChars = ".'`"
let reTrim = RegExp('^[' + trimChars + ']+|[' + trimChars + ']+$', 'g')

function fixWord(word) {
	return word
		.toLowerCase()
		.replace(reTrim, '')
		.replace(/^(\d{4})г/, '$1') //2007г -> 2007
}

let skipWords = new Set(['', '-', '&'])

function splitTextToLexemes(text) {
	return text.replace(/\.\.\./g, '…')
		.replace(/(\D)\.|\.(\D)/g, '$1 $2') //qwe.asd -> qwe asd | 12.34 -> 12.34
		.split(/[\s\/\\()\[\]{}<>;"~+\-*=!?,#_:|«»„“”‘’™²…—–•·\u00AD]+/)
		.map(fixWord)
		.filter(x => !skipWords.has(x))
		.map(stemWord)
}

module.exports = { splitTextToLexemes }
