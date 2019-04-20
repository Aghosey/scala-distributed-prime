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
 docker -v ~/primes.txt:/code/primes.txt \
        -v ~/checkPoint.bin:/code/checkPoint.bin \ -p 9999:9999 x-server 
```

Similarly to run server
```
 docker -v ~/primes.txt:/code/primes.txt \
        -v ~/checkPoint.bin:/code/checkPoint.bin \ x-client 
```

