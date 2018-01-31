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

/*
param: {
  rowDelimiter (String) used to split lines default: "\n\r"
  delimiter (String) used to separate each column
  header (boolean) display header or not default false

  displayEmptyValue (Boolean) display header or not
  columns (array) [
    column (Object) {
      name (String) the name of the column
      header (string) display in header default name
      type (String) the type of the column possible value are:
        - "string"
        - "date"
        - "number"
        - "boolean"
        - custom type (require formatter)
      formatter (function(value) return String) function that return a formatted string for this column overide formatters default
    }
  ]
  formatters (object) { default formatters for each type
    string (function)
    date (function)
    number (function)
    boolean (function)
    custom (funtion) use for custom type
  }
}

*/

function exportCsv(param) {
  var self = this;

  this.rowDelimiter = param.rowDelimiter || "\n\r";
  this.delimiter = param.delimiter || ";";
  this.showHeaders = !!param.showHeaders;
  this.headersSent = false;
  this.displayEmptyValue = param.displayEmptyValue || "";
  this.columns = param.columns || [];
  this.result = '';

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
    this.result += headers;
    this.headers = headersObject;
  }

  this.processLine = through2.obj(function(chunk, enc, callback) {
    var line = "";
    var index = 0;
    asynk.each(self.columns, function(column, cb) {
      var data = chunk[column.name];

      if (self.sendingHeader) {
        var formatter = self.formatters['doNothing'];
      } else {
        var formatter = column.formatter || self.formatters[column.type];
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
      if (line !== "") {
        line += self.rowDelimiter;
        if (self.lineCb) {
          self.lineCb(line, function() {
            callback(null, chunk.toString());
          });
        } else {
          self.result += (line);
          callback(null, chunk.toString());
        }
      } else {
        callback(null, chunk.toString());
      }
    });
  });

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
}

exportCsv.prototype.setLineFn = function(fn) {
  this.lineCb = fn;
};

exportCsv.prototype.process = function(input) {
  if (this.noMoreInput) {
    return;
  }
  if (this.showHeaders && !this.headersSent && this.lineCb) {
    this.headersSent = true;
    this.sendingHeader = true;
    this.entryStream.write(this.headers);
  }
  if (isReadableStream(input)) {
    this.inputIsStream = true;
    this.noMoreInput = true;
    input.pipe(this.entryStream);
  } else {
    this.entryStream.write(input);
  }
};

exportCsv.prototype.end = function(cb) {
  this.noMoreInput = true;
  if (!this.inputIsStream) {
    this.entryStream.end();
  }
  this.processLine.pipe(fs.createWriteStream('/dev/null'));
  return this.processLine;
};

module.exports = exportCsv;
