var _ = require('lodash');
var csv = require('csv');
var through2 = require('through2');
var iconv = require('iconv-lite');
var asynk = require('asynk');

var encodingCorrespondences = {
  'windows-1252': 'win1252',
  'ascii': 'utf8'
};

function importCsv(options) {
  if (!options || !options.columns || !options.columns.length) {
    throw new Error('no columns defined');
  }

  var self = this;
  this.result = [];
  this.options = options;
  this.columns = options.columns;
  this.skipFirstLine = options.skipFirstLine;
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
      if (!_.isDate(val)) {
        return new Date(val);
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

  this.typeChecks = {
    string: function(column, val, originalVal, rowNum) {
      if (_.isUndefined(val) || _.isNull(val) || _.isString(val)) {
        return val;
      } else {
        return new Error(originalVal + ' on row ' + rowNum + ' is not a valid value for column ' + column.name + ' of type ' + column.type);
      }
    },
    date: function(column, val, originalVal, rowNum) {
      if (_.isUndefined(val) || _.isNull(val) || _.isDate(val)) {
        return val;
      } else {
        return new Error(originalVal + ' on row ' + rowNum + ' is not a valid value for column ' + column.name + ' of type ' + column.type);
      }
    },
    number: function(column, val, originalVal, rowNum) {
      if (_.isUndefined(val) || _.isNull(val) || _.isFinite(val)) {
        return val;
      } else {
        return new Error(originalVal + ' on row ' + rowNum + ' is not a valid value for column ' + column.name + ' of type ' + column.type);
      }
    }
  };

  this.columnNames = [];
  options.columns.forEach(function(col) {
    self.columnNames.push(col.name);
  });

  if (options.formatters) {
    Object.keys(options.formatters).forEach(function(formName) {
      self.formatters[formName] = options.formatters[formName];
    });
  }

  if (options.typeChecks) {
    Object.keys(options.typeChecks).forEach(function(typeName) {
      self.typeChecks[typeName] = options.typeChecks[typeName];
    });
  }

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
    callback(null, dataChunk);
  };

  if (options.inputMode === 'object') {
    this.entryStream = through2.obj(entryProcess);
  } else {
    this.entryStream = through2(entryProcess);
  }

  this.processLine.nbOfPipes = 0;
  this.processLine._pipe = this.processLine.pipe;
  this.processLine._unpipe = this.processLine.unpipe;

  this.processLine.pipe = function(stream) {
    ++self.processLine.nbOfPipes;
    self.processLine._pipe(stream);
  };

  this.processLine.unpipe = function(stream) {
    --self.processLine.nbOfPipes;
    self.processLine._unpipe(stream);
  };

  this.entryStream
  .pipe(iconv.decodeStream(encodingCorrespondences[options.encoding] || 'utf8'))
  .pipe(self.parser)
  .pipe(self.transformer)
  .pipe(self.processLine);

  this.entryStream.getOutput = function(fn) {
    if (fn) {
      self.lineCb = fn;
    }
    return self.processLine;
  };

  this.entryStream.getInfo = function(data, cb) {
    csv.parse(data,
      {
        columns: self.columnNames || null,
        delimiter: self.options.delimiter || ';',
        skip_empty_lines: true,
        relax: true,
        relax_column_count: true,
        rowDelimiter: self.options.rowDelimiter || '\n',
        from: self.options.from || 1
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

  return this.entryStream;
}

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

  var rowNum = null;
  pipes.processLine = through2.obj(function(chunk, enc, callback) {
    if (self.skipFirstLine && _.isNull(rowNum)) {
      rowNum = 0;
      return callback();
    }
    rowNum = rowNum || 0;
    ++rowNum;
    var streamContext = this;
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
        data = err;
      }

      var typeChecker = column.typeCheck || self.typeChecks[column.type];

      if (_.isError(data) || data == 'Invalid Date') {
        data = new Error(dataChunk[column.name] + ' on row ' + rowNum + ' is not a valid value for column ' + column.name + ' of type ' + column.type);
      } else if (typeChecker && _.isFunction(typeChecker)) {
        data = typeChecker(column, data, dataChunk[column.name], rowNum);
      }

      if (_.isError(data)) {
        data.code = 'ERR_CSV_IO_INVALID_VALUE';
        data.meta = {
          value: dataChunk[column.name],
          columnName: column.name,
          columnType: column.type,
          rowNum: rowNum
        };
        return cb(data);
      }

      parsedLine[column.name] = data;
      cb();
    }).serie().done(function() {
      var pipeCallback = function(err) {
        callback(err);
      };
      if (streamContext.nbOfPipes) {
        pipeCallback = function(err) {
          callback(err, parsedLine);
        };
      }
      if (self.lineCb) {
        self.lineCb(parsedLine, function(err) {
          pipeCallback(err);
        });
      } else {
        pipeCallback();
      }
    }).fail(function(err) {
      streamContext.emit('error', err);
    });
  });

  return pipes;
};

module.exports = importCsv;
