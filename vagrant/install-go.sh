wget https://storage.googleapis.com/golang/go1.4.2.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.4.2.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc

mkdir ~/GoPkgs
echo 'export GOPATH=~/GoPkgs' >> ~/.bashrc

source ~/.bashrc

go version
go get github.com/mattn/go-sqlite3
