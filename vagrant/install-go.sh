wget https://storage.googleapis.com/golang/go1.4.2.linux-amd64.tar.gz --progress=bar:force
sudo tar -C /usr/local -xzf go1.4.2.linux-amd64.tar.gz
rm -f go1.4.2.linux-amd64.tar.gz

mkdir /home/vagrant/GoPkgs
export PATH=$PATH:/usr/local/go/bin
export GOPATH=/home/vagrant/GoPkgs

go version
go get github.com/mattn/go-sqlite3

echo 'export PATH=$PATH:/usr/local/go/bin' >> /home/vagrant/.bashrc
echo 'export GOPATH=~/GoPkgs' >> /home/vagrant/.bashrc
