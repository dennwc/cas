```
## conda install -y go
## conda install -y gcc --force-reinstall
git clone https://github.com/dennwc/cas.git
cd cas
go get -u
go build ./*.go
cd cmd/cas
go get -u
go build ./*.go
ln blobs cas
## Move cas to a executable directory. 
## ln cas ~/bin/
```
