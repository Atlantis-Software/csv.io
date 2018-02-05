var _ = require('lodash');
var through2 = require('through2');
var asynk = require('asynk');
var stream = require('stream');
var fs = require('fs');

function isReadableStream(obj) {
  return obj instanceof stream.Stream &&
    typeof (obj._read === 'function') &&
    typeof (obj._readableState === 'object');
}

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
  this.formatters = param.formatters || {};

  this.formatters.string = this.formatters.string || function(column, val) {
    if (_.isNull(val) && column.nullable) {
      return 'null';
    }
    if (_.isUndefined(val) || _.isNull(val) || (_.isString(val) && !val.length)) {
      return self.displayEmptyValue;
    }
    return '"' + val + '"';
  };

  this.formatters.date = this.formatters.date || function(column, val) {
    if (_.isNull(val) && column.nullable) {
      return 'null';
    }
    if (_.isUndefined(val) || _.isNull(val) || (_.isString(val) && !val.length)) {
      return self.displayEmptyValue;
    }
    return val.toString();
  };

  this.formatters.number = this.formatters.number || function(column, val) {
    if (_.isNull(val) && column.nullable) {
      return 'null';
    }
    if (_.isUndefined(val) || _.isNull(val) || (_.isString(val) && !val.length)) {
      return self.displayEmptyValue;
    }
    return val;
  };

  this.formatters.boolean = this.formatters.boolean || function(column, val) {
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
  };

  this.formatters.doNothing = function(column, val) {
    return val;
  };

  var headers = "";
  var headersObject = {};

  this.columns.forEach(function(column, index) {
    if (index > 0) {
      headers += self.delimiter;
    }
    if (!column.name) {
      throw new Error('A column has no name');
    }
    if (!column.type) {
      throw new Error('Column ' + column.name + ' has no type');
    }

    column.header = column.header || column.name;

    headers += column.header;
    headersObject[column.header] = column.header;
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
      var pipeCallback = function() {
        callback();
      };
      if (streamContext.nbOfPipes) {
        pipeCallback = function() {
          callback(null, line);
        };
      }
      if (line !== "") {
        line += self.rowDelimiter;
        if (self.lineCb) {
          self.lineCb(line, function() {
            pipeCallback();
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

// exportCsv.prototype.setLineFn = function(fn) {
//   this.lineCb = fn;
// };
//
// exportCsv.prototype.process = function(input) {
//   var self = this;
//   if (this.noMoreInput) {
//     return;
//   }
//   if (this.showHeaders && !this.headersSent && this.lineCb) {
//     this.headersSent = true;
//     this.sendingHeader = true;
//     this.entryStream.write(this.headers);
//   }
//   if (isReadableStream(input)) {
//     this.inputIsStream = true;
//     this.noMoreInput = true;
//     input.pipe(this.entryStream);
//   } else if (_.isArray(input)) {
//     input.forEach(function(line) {
//       self.entryStream.write(line);
//     });
//   } else {
//     this.entryStream.write(input);
//   }
// };
//
// exportCsv.prototype.end = function() {
//   this.noMoreInput = true;
//   if (!this.inputIsStream) {
//     this.entryStream.end();
//   }
//   this.processLine.pipe(fs.createWriteStream('/dev/null'));
//   return this.processLine;
// };

module.exports = exportCsv;
