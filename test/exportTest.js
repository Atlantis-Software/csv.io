var path = require('path');
var _ = require('lodash');
var assert = require('assert');

describe('Export', function() {
  var ExportCsv = require(path.join(__dirname, '..', 'index.js')).exporter;

  var data = [
    {
      id: 1,
      someString: 'blabla1',
      someDate: new Date('1995-01-01'),
      someNumber: 123,
      someBoolean: 1
    },
    {
      id: 2,
      someString: 'blabla2',
      someDate: new Date('1996-01-01'),
      someNumber: 456,
      someBoolean: 0
    },
    {
      id: 3,
      someString: 'blabla3',
      someDate: new Date('1997-01-01'),
      someNumber: 789,
      someBoolean: 1
    },
    {
      id: 4,
      someString: '',
      someDate: '',
      someNumber: null,
      someBoolean: null
    },
    {
      id: 5
    }
  ];

  var expectedResultLines = [
    '1;"blabla1";Sun Jan 01 1995 01:00:00 GMT+0100 (CET);123;1\n',
    '2;"blabla2";Mon Jan 01 1996 01:00:00 GMT+0100 (CET);456;0\n',
    '3;"blabla3";Wed Jan 01 1997 01:00:00 GMT+0100 (CET);789;1\n',
    '4;;;null;\n',
    '5;;;;\n'
  ]

  var expectedResult = `1;"blabla1";Sun Jan 01 1995 01:00:00 GMT+0100 (CET);123;1
2;"blabla2";Mon Jan 01 1996 01:00:00 GMT+0100 (CET);456;0
3;"blabla3";Wed Jan 01 1997 01:00:00 GMT+0100 (CET);789;1
4;;;null;
5;;;;
`

  var options = {
    rowDelimiter: '\n',
    columns: [
      {name: 'id', type: 'number'},
      {name: 'someString', type: 'string'},
      {name: 'someDate', type: 'date'},
      {name: 'someNumber', type: 'number', nullable: true},
      {name: 'someBoolean', type: 'boolean'}
    ]
  };

  it('should transform json object into csv', function(done) {
    var exportCsv = new ExportCsv(options);

    data.forEach(function(line) {
      exportCsv.process(line);
    });

    exportCsv.end()
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      assert.equal(exportCsv.result, expectedResult);
      done();
    });
  });

  it('should transform json object line by line into csv', function(done) {
    var exportCsv = new ExportCsv(options);
    var i = 0;

    exportCsv.setLineFn(function(line, cb) {
      assert.equal(line, expectedResultLines[i++], 'Returned csv string line should be correct');
      cb();
    });

    data.forEach(function(line) {
      exportCsv.process(line);
    });

    exportCsv.end()
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      done();
    });
  });

  it('should transform json object into csv with headers', function(done) {
    options.showHeaders = true;
    var exportCsv = new ExportCsv(options);

    data.forEach(function(line) {
      exportCsv.process(line);
    });

    exportCsv.end()
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      assert.equal(exportCsv.result, 'id;someString;someDate;someNumber;someBoolean\n' + expectedResult);
      done();
    });
  });

  it('should transform json object line by line into csv with headers', function(done) {
    options.showHeaders = true;
    var exportCsv = new ExportCsv(options);
    var i = 0;
    var firstLine = true;

    exportCsv.setLineFn(function(line, cb) {
      if (firstLine) {
        firstLine = false;
        assert.equal(line, 'id;someString;someDate;someNumber;someBoolean\n', 'First line should have headers');
      } else {
        assert.equal(line, expectedResultLines[i++], 'Returned csv string line should be correct');
      }
      cb();
    });

    data.forEach(function(line) {
      exportCsv.process(line);
    });

    exportCsv.end()
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      done();
    });
  });

  it('should work with a stream even if the internal buffer get full', function(done) {
    options.showHeaders = false;
    var exportCsv = new ExportCsv(options);
    var i = 0;

    exportCsv.setLineFn(function(line, cb) {
      cb();
    });

    var fatData = [];
    for (var y = 0; y < 50; ++y) {
      fatData.push(data);
    }
    fatData = _.flattenDeep(fatData);

    fatData.forEach(function(line) {
      exportCsv.process(line);
    });

    exportCsv.end()
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      done();
    });
  });

  it('should work with a stream even if the internal buffer get full', function(done) {
    options.showHeaders = false;
    var exportCsv = new ExportCsv(options);
    var i = 0;

    exportCsv.setLineFn(function(line, cb) {
      cb();
    });

    var fatData = [];
    for (var y = 0; y < 50; ++y) {
      fatData.push(data);
    }
    fatData = _.flattenDeep(fatData);

    fatData.forEach(function(line) {
      exportCsv.process(line);
    });

    exportCsv.end()
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      done();
    });
  });
});
