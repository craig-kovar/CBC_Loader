# CBC_Loader
Simple Command Line tool to load and test Couchbase

##Usage
CBC_Loader
Version: 1.0.0

Required Settings
	-m mode - The execution mode.  Valid values are fakeit, load, and perf
	-c connection string - The Couchbase connection string
	-u username - The username to connect to Couchbase
	-p password - The password to connect to Couchbase
	-t threads - The number of threads to use

Mode = fakeit
	-d directory - The directory where the documents will be loaded from

Mode = load
	-i id pattern = The pattern to use for ids = I-%d
	-o id offset = The starting value to use for ids
	-P = print output

Mode = perf
	-B batch size = The number of gets to perform per batch
	-i id pattern = The pattern to use for ids = I-%d
	-r min id = The minimum id range to get
	-R max id = The maximum id range to get
	-a = Use Async Couchbase API
	-P = print output
  
  ##Mode _fakeit_
  This mode is designed to read a directory output from the [fakeit](https://github.com/bentonam/fakeit.git) utility and load the data into Couchbase
  
  ##Mode _load_
  This method is designed to read the first 10,000 documents or until a DocumentNotFound exception is encountered.  It will then continously add new documents using the values from the first 10,000 documents.
  
  ##Mode _perf_
  This mode is designed to perform Couchbase Get operations from a starting id to a maximum id.  This does support using both the synchronous and aysnchronous APIs
