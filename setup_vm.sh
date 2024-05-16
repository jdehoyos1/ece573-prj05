#!/bin/bash

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
apt-get update
apt-get install -y docker-ce docker-compose-plugin
usermod -aG docker ubuntu

curl -OL https://go.dev/dl/go1.21.0.linux-amd64.tar.gz
tar -C /usr/local -xvf go1.21.0.linux-amd64.tar.gz
rm go1.21.0.linux-amd64.tar.gz

curl -LO "https://dl.k8s.io/release/v1.30.0/bin/linux/amd64/kubectl"
install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
rm kubectl

echo 'export PATH=$PATH:/usr/local/go/bin:/home/ubuntu/go/bin' >> /home/ubuntu/.bashrc

sudo -i -u ubuntu bash << EOF
/usr/local/go/bin/go install sigs.k8s.io/kind@v0.23.0
EOF

su - ubuntu
