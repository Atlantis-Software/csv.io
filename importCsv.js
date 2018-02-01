var _ = require('lodash');
var csv = require('csv');
var through2 = require('through2');
var iconv = require('iconv-lite');
var asynk = require('asynk');
var fs = require('fs');
var stream = require('stream');

var encodingCorrespondences = {
  'windows-1252': 'win1252',
  'ascii': 'utf8'
};

function isReadableStream(obj) {
  return obj instanceof stream.Stream &&
    typeof (obj._read === 'function') &&
    typeof (obj._readableState === 'object');
}

function importCsv(options) {
  if (!options || !options.columns || !options.columns.length) {
    throw new Error('no columns defined');
  }

  var self = this;
  this.result = [];
  this.noMoreInput = false;
  this.inputIsStream = false;
  this.options = options;
  this.columns = options.columns;
  this.emptyValue = options.emptyValue || '';
  this.formatters = {
    string: function(column, val) {
      if ((val === 'null' || _.isNull(val)) && column.nullable) {
        return null;
      }
      if (_.isUndefined(val) || _.isNull(val) || (_.isString(val) && !val.length) || val == 'null') {
        return void 0;
      }
      return val;
    },
    date: function(column, val) {
      if ((val === 'null' || _.isNull(val)) && column.nullable) {
        return null;
      }
      if (_.isUndefined(val) || _.isNull(val) || val == 'null') {
        return void 0;
      }
      return val;
    },
    number: function(column, val) {
      if ((val === 'null' || _.isNull(val)) && column.nullable) {
        return null;
      }
      if (_.isUndefined(val) || _.isNull(val) || (_.isString(val) && !val.length) || val == 'null') {
        return void 0;
      }
      if (_.isString(val)) {
        val = val.replace(/,/g, '.');
        return parseFloat(val);
      }
      return val;
    },
    boolean: function(column, val) {
      if ((val === 'null' || _.isNull(val)) && column.nullable) {
        return null;
      }
      if (_.isUndefined(val) || _.isNull(val) || val == 'null') {
        return void 0;
      }
      return !!val;
    },
    doNothing: function(column, val) {
      return val;
    }
  };

  this.columnNames = [];
  options.columns.forEach(function(col) {
    self.columnNames.push(col.name);
  });

  var pipes = this.getPipes(options);
  this.parser = pipes.parser;
  this.transformer = pipes.transformer;
  this.processLine = pipes.processLine;

  var entryProcess = function(chunk, enc, callback) {
    var dataChunk = chunk;
    while (dataChunk.length > 1000) {
      this.push(dataChunk.slice(0, 1000));
      dataChunk = dataChunk.slice(1000, dataChunk.length);
    }
    this.push(dataChunk);
    callback();
  };

  if (options.inputMode === 'object') {
    this.entryStream = through2.obj(entryProcess);
  } else {
    this.entryStream = through2(entryProcess);
  }

  this.entryStream
  .pipe(iconv.decodeStream(encodingCorrespondences[options.encoding] || 'utf8'))
  .pipe(self.parser)
  .pipe(self.transformer)
  .pipe(self.processLine);
}

importCsv.prototype.setLineFn = function(fn) {
  this.lineCb = fn;
};

importCsv.prototype.process = function(input) {
  if (this.noMoreInput) {
    return;
  }
  if (isReadableStream(input)) {
    this.inputIsStream = true;
    this.noMoreInput = true;
    input.pipe(this.entryStream);
  } else {
    this.entryStream.write(input);
  }
};

importCsv.prototype.end = function() {
  this.noMoreInput = true;
  if (!this.inputIsStream) {
    this.entryStream.end();
  }
  this.processLine.pipe(fs.createWriteStream('/dev/null'));
  return this.processLine;
};

importCsv.prototype.info = function(data, cb) {
  csv.parse(data,
    {
      columns: this.columnNames || null,
      delimiter: this.options.delimiter || ';',
      skip_empty_lines: true,
      relax: true,
      relax_column_count: true,
      rowDelimiter: this.options.rowDelimiter || '\n',
      from: this.options.from || 1
    }, function(err, result) {
      var bytes = 0;
      var sizeOf = function (obj) {
        if (obj !== null && obj !== undefined) {
          switch (typeof obj) {
            case 'number':
              bytes += 8;
              break;
            case 'string':
              bytes += obj.length * 2;
              break;
            case 'boolean':
              bytes += 4;
              break;
            case 'object':
              if (_.isObject(obj)) {
                for (var key in obj) {
                  if (!obj.hasOwnProperty(key)) continue;
                  sizeOf(obj[key]);
                }
              } else {
                bytes += obj.toString().length * 2;
              }
              break;
          }
        }
        return bytes;
      };
      cb(err, {entriesCount: result.length, dataSize: sizeOf(result)});
    });
};

importCsv.prototype.getPipes = function(options) {
  var self = this;
  var pipes = {};

  pipes.parser = csv.parse({
    columns: self.columnNames || null,
    delimiter: options.delimiter || ';',
    skip_empty_lines: true,
    relax: true,
    relax_column_count: true,
    rowDelimiter: options.rowDelimiter || '\n',
    from: options.from || 1
  });

  pipes.transformer = csv.transform(data => {
    if (_.isArray(data) && !options.rowDelimiter) {
      var replace = [];
      var isEmpty = true;
      for (var i = 0; i < data.length; ++i) {
        var dirty = data[i].toString();
        replace[i] = dirty.replace(/\"\\r/g, '"')
          .replace(/\\r\"/g, '"')
          .replace(/^\\r\\n/g, '')
          .replace(/\\r\\n$/g, '')
          .replace(/^\\r/g, '')
          .replace(/\\r$/g, '')
          .replace(/^\\n/g, '')
          .replace(/\\n$/g, '');
        if (replace[i] || replace[i] === 0) {
          isEmpty = false;
        }
      }
      if (isEmpty) {
        return null;
      }
      return replace;
    } else if (typeof data === 'string') {
      data = data.replace(/\"\\r/g, '"')
        .replace(/\\r\"/g, '"')
        .replace(/^\\r\\n/g, '')
        .replace(/\\r\\n$/g, '')
        .replace(/^\\r/g, '')
        .replace(/\\r$/g, '')
        .replace(/^\\n/g, '')
        .replace(/\\n$/g, '');
      return data;
    } else if (typeof data === 'object') {
      data = JSON.stringify(data);
      data = data.replace(/\"\\r/g, '"')
        .replace(/\\r\"/g, '"')
        .replace(/^\\r\\n/g, '')
        .replace(/\\r\\n$/g, '')
        .replace(/^\\r/g, '')
        .replace(/\\r$/g, '')
        .replace(/^\\n/g, '')
        .replace(/\\n$/g, '');
      data = JSON.parse(data);
      return data;
    } else {
      if (!data) {
        return null;
      }
      return data;
    }
  });

  pipes.processLine = through2.obj(function(chunk, enc, callback) {
    var dataChunk = chunk;
    var parsedLine = {};
    asynk.each(self.columns, function(column, cb) {
      var data = dataChunk[column.name];
      var formatter = column.formatter || self.formatters[column.type];

      if (!formatter || !_.isFunction(formatter)) {
        throw new Error('no formatter for column ' + column.name);
      }
      try {
        data = formatter(column, data);
      } catch (err) {
        console.log('Formatting error : ', err);
      }
      parsedLine[column.name] = data;
      cb();
    }).serie().done(function() {
      if (self.lineCb) {
        self.lineCb(parsedLine, function() {
          callback(null, chunk.toString());
        });
      } else {
        self.result.push(parsedLine);
        callback(null, chunk.toString());
      }
    });
  });

  return pipes;
};

module.exports = importCsv;
