var Snowball = require('snowball');
window.Snowball = Snowball
var stemmer = new Snowball('English');
stemmer.setCurrent('abbreviations');
stemmer.stem();
alert(stemmer.getCurrent());
var stemmer = new Snowball('Russian');
stemmer.setCurrent('яблоки');
stemmer.stem();
alert(stemmer.getCurrent());
