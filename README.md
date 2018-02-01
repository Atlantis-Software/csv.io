# csv.io
csv import / export

Csv.io is a node module that provide functions simplifying the import/export of csv. It uses streams and handle backpressure, wich provide good scalability. You can also provide your own formatters, to get exactly the data you want.

## Installation
Just do an npm install in your project folder :
```
npm install csv.io
```

## Usage
Csv.io is composed of two instantiable objects : an importer and an exporter. They don't take the same options, and must be instantiated separately.

The importer take a csv string/buffer/stream and convert it to json object. The exporter do the opposite.

But first let's require everything you need :
```javascript
var csvIO = require('csv.io');

var importer = csvIO.importer;
var exporter = csvIO.exporter;
```
Now for the specific usage of each object :

### Importer
The initialization of the importer require an object containing some options. Let's take an example :

```javascript
var options = {
  columns: [
    { name: 'column1', type: 'string' },
    { name: 'column2', type: 'number' },
    { name: 'column3', type: 'string', nullable: true },
    { name: 'column4', type: 'boolean' },
    { name: 'column5', type: 'date' },
    { name: 'column6', type: 'doNothing' },
    { name: 'column7', formatter: myCustomFormatter }
  ],
  encoding: 'windows-1252', // can also be 'ascii', and default to utf-8
  delimiter: '|', // default to ';',
  rowDelimiter: '\r\n' // default to '\n'
};
```
The only option really required is `columns`, wich is an array of objects with a `name`, and either a `type` (that is basically a standard formatter for `string`, `number`, `boolean`, or `date`. `doNothing` is a special formatter that just return the data without formatting it) or a `formatter` property where you can inject your own formatter.

You can also define a `nullable` property. If a field is nullable and contain null or 'null', it will be returned as null, if it's not nullable it will be returned as an empty value. Default to false.

A custom fomatter is a function. So if we take the previous example, `myCustomFormatter` could look like this :

```javascript
var myCustomFormatter = function(column, value) {
  if (isNull(val)) && column.nullable) {
    return null;
  }
  if (isEmpty(val) || isNull(val)) {
    return void 0;
  }
  return 'this value -> ' + val + ' <- is now modified';
}
```

`column` is basically the column you passed in the options (so you can check if it's nullable, or even pass other custom metadata), `value` is the value parsed from the csv.

Once everything is set, you can initialize the importer :
```javascript
var importCsv = new importer(options);
```

You must then set the function that will be called on each fully parsed line, with `setLineFn` :

```javascript
  importCsv.setLineFn(function(line, cb) {
    insertIntoDb(line); // Or whatever you want to do with your new shiny object
    return cb(); // Don't forget to call the callback
  });
```

You can then start feeding data to your importer, through the `process` function. You can feed it either multiple buffer or strings...
```javascript
importCsv.process(someBuffer);
importCsv.process(someOtherBuffer);
importCsv.process(someString);
```

... or single stream, but take note that as soon as you feed it a stream, this instance of the importer won't accept input anymore :

```javascript
importCsv.process(someStream);
importCsv.process(anotherBuffer); // The importer will just ignore this input
```

The actual processing hasn't started yet. To start it, use the `end` function, wich will start the processing and return a stream so you can listen for `error` and `finish` event (**DO NOT** listen for the `data` event or you will break all backpressure handling and will flood your memory, use the `setLineFn` function instead).

```javascript
importCsv.end()
.on('error', function(err) {
  // Handle your errors here
})
.on('finish', function() {
  // you can has cheezburger
});
```

Obviously, once you called `end`, you cannot call `process` anymore.

### Exporter
The exporter is really similar in usage to the importer, but its options are slightly differents :

```javascript
var options = {
  columns: [
    { name: 'column1', type: 'string' },
    { name: 'column2', type: 'number' },
    { name: 'column3', type: 'string', nullable: true },
    { name: 'column4', type: 'boolean' },
    { name: 'column5', type: 'date' },
    { name: 'column6', type: 'doNothing' },
    { name: 'column7', formatter: myCustomFormatter }
  ],
  delimiter: '|', // default to ';',
  rowDelimiter: '\r\n', // default to '\n'
  showHeaders: true, // First line returned will be the columns names, default to false
  displayEmptyValue: 'EMPTY' // Value displayed when a field is empty, default to empty string
};
```

Once again, only `columns` is required.
Like the importer, you can now instantiate the exporter :

```javascript
exportCsv = new exporter(options);
```

There is two way to use the exporter : either you define a line by line function to call with `setLineFn`, or you don't and you get the result in one block at the end. The latter is not recommended with big volume as it will quickly fill the memory.

As with the importer, you feed data through `process`. It accepts either an object or an array of objects.

Example without line callback :
```javascript
exportCsv.process(data);

exportCsv.end()
.on('error', function(err) {
  // Handle errors here
})
.on('finish', function() {
  console.log(exportCsv.result); // You can get your processed result here
});
```

Example with line callback (recommended) :
```javascript
exportCsv.setLineFn(function(line, cb) {
  // Do whatever you want with the csv line
  cb();
});

exportCsv.process(data);

exportCsv.end()
.on('error', function(err) {
  // Handle errors here
})
.on('finish', function() {
  console.log(exportCsv.result); // Result is empty
});
```
