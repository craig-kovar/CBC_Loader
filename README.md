# CBC_Loader
Simple Command Line tool to load and test Couchbase

## Usage
CBC_Loader

Version 1.0.0

Required Settings

	-m mode = The execution mode. Valid values are fakeit, load, and perf
	-c connection url = The Couchbase Connection Url
	-u username = The username to connect to Couchbase
	-p password = The password to connect to Couchbase
	-t threads = The number of threads to use
	
Mode = fakeit

	-d directory = The directory where the documents will be loaded from
	
Mode = load

	-i id_pattern = The pattern to use for ids; i.e. I-%d
	-o id_offset = The starting value to use with ids for the new documents

Mode = perf

	-B batch_size = The batch size when using the Reactive api
	-i id_pattern = The pattern to use for ids; i.e. I-%d
	-r min_id = The starting numeric value for the ids to fetch.  Used with id_pattern
	-R max_id = The maximum numeric value for the ids to fetch. Used with id_pattern
	-a = Enables reactive sdk
	-P = print the documents that are retrieved
  
  ## Mode _fakeit_
  This mode is designed to read a directory output from the [fakeit](https://github.com/bentonam/fakeit.git) utility and load the data into Couchbase
  
  ## Mode _load_
  This method is designed to read the first 10,000 documents or until a DocumentNotFound exception is encountered.  It will then continously add new documents using the values from the first 10,000 documents.
  
  ## Mode _perf_
  This mode is designed to perform Couchbase Get operations from a starting id to a maximum id.  This does support using both the synchronous and aysnchronous APIs
