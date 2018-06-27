var _ = require('lodash');
var through2 = require('through2');
var asynk = require('asynk');

function exportCsv(param) {
  var self = this;

  this.rowDelimiter = param.rowDelimiter || "\n\r";
  this.delimiter = param.delimiter || ";";
  this.showHeaders = !!param.showHeaders;
  this.displayEmptyValue = param.displayEmptyValue || "";
  this.columns = param.columns || [];

  if (this.columns.length === 0) {
    throw new Error('no column defined');
  }

  this.formatters = {
    string: function(column, val) {
      if (_.isNull(val) && column.nullable) {
        return 'null';
      }
      if (_.isUndefined(val) || _.isNull(val) || (_.isString(val) && !val.length)) {
        return self.displayEmptyValue;
      }
      val = val.replace(/"/g, '""');
      return '"' + val + '"';
    },
    date: function(column, val) {
      if (_.isNull(val) && column.nullable) {
        return 'null';
      }
      if (_.isUndefined(val) || _.isNull(val) || (_.isString(val) && !val.length)) {
        return self.displayEmptyValue;
      }
      return val.toString();
    },
    number: function(column, val) {
      if (_.isNull(val) && column.nullable) {
        return 'null';
      }
      if (_.isUndefined(val) || _.isNull(val) || (_.isString(val) && !val.length)) {
        return self.displayEmptyValue;
      }
      return val;
    },
    boolean: function(column, val) {
      if (_.isNull(val) && column.nullable) {
        return 'null';
      }
      if (_.isUndefined(val) || _.isNull(val) || (_.isString(val) && !val.length)) {
        return self.displayEmptyValue;
      }
      if (val) {
        return "1";
      }
      return "0";
    },
    doNothing: function(column, val) {
      return val;
    }
  };

  if (param.formatters) {
    Object.keys(param.formatters).forEach(function(formName) {
      self.formatters[formName] = param.formatters[formName];
    });
  }

  var headers = "";
  var headersObject = {};

  this.columns.forEach(function(column, index) {
    if (index > 0) {
      headers += self.delimiter;
    }
    if (!column.name) {
      throw new Error('A column has no name');
    }
    if (!column.type && !column.formatter) {
      throw new Error('Column ' + column.name + ' has no type nor formatter');
    }

    column.header = column.header || column.name;

    headers += column.header;
    headersObject[column.name] = column.header;
  });
  headers += this.rowDelimiter;
  // write headers
  if (this.showHeaders) {
    this.headers = headersObject;
  }

  this.processLine = through2.obj(function(chunk, enc, callback) {
    var streamContext = this;
    var line = "";
    var index = 0;
    asynk.each(self.columns, function(column, cb) {
      var data = chunk[column.name];

      var formatter;
      if (self.sendingHeader) {
        formatter = self.formatters['doNothing'];
      } else {
        formatter = column.formatter || self.formatters[column.type];
      }
      if (!formatter || !_.isFunction(formatter)) {
        throw new Error('no formatter for column ' + column.name);
      }
      if (index > 0) {
        line += self.delimiter;
      }
      ++index;
      try {
        data = formatter(column, data);
      } catch (err) {
        console.log('Formatting error : ', err);
      }
      line += data;
      cb();
    }).serie().done(function() {
      if (self.sendingHeader) {
        self.sendingHeader = false;
      }
      var pipeCallback = function(err) {
        callback(err);
      };
      if (streamContext.nbOfPipes) {
        pipeCallback = function(err) {
          callback(err, line);
        };
      }
      if (line !== "") {
        line += self.rowDelimiter;
        if (self.lineCb) {
          self.lineCb(line, function(err) {
            pipeCallback(err);
          });
        } else {
          pipeCallback();
        }
      } else {
        pipeCallback();
      }
    });
  });

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

  var entryProcess = function(chunk, enc, callback) {
    var dataChunk = chunk;
    while (dataChunk.length > 1000) {
      this.push(dataChunk.slice(0, 1000));
      dataChunk = dataChunk.slice(1000, dataChunk.length);
    }
    this.push(dataChunk);
    callback();
  };

  this.entryStream = through2.obj(entryProcess)
  .pipe(self.processLine);

  this.entryStream.getOutput = function(fn) {
    if (fn) {
      self.lineCb = fn;
    }
    return self.processLine;
  };

  if (this.showHeaders && this.headers) {
    this.sendingHeader = true;
    this.entryStream.write(this.headers);
  }

  return this.entryStream;
}

module.exports = exportCsv;
