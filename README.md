Distributed prime numbers computation using scala, with 
wheel sieve algorithm, uses grpc as client server protocol


To build server
```
 docker build . -t x-server --file Dockerfile.server 
```
To build client
```
 docker build . -t x-client --file Dockerfile.client 
```
Use whatever name instead of x-client and server

To run server
```
 docker run -v ~/bigdata-files:/code/files -P x-server
```
Note that `~/bigdata-files` is the directory for server backup and output files

Similarly to run client
```
 docker run x-client 
```

