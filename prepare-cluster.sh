#!/bin/bash
dir="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$dir"
source $dir/common.sh


aptstuff () {
  if [ "$1" == "localhost" ]
  then
  # Increase apt cache limit, install basic versions of deps
  echo 'APT::Cache-Limit 50000000;' | sudo tee /etc/apt/apt.conf
  sudo apt-get update
  sudo aptitude -y install $( cat deps.txt )

  # Enable Debian Unstable, install scala 2.9.1 or later
  echo 'deb http://ftp.debian.org/debian unstable main contrib non-free
deb-src http://ftp.debian.org/debian unstable main contrib non-free' | sudo tee /etc/apt/sources.list.d/debian-unstable-for-scala.list
  sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys AED4B06F473041FA
  sudo apt-get update
  sudo apt-get -y install scala

  # Reset apt sources to current distribution
  echo '# deb http://ftp.debian.org/debian unstable main contrib non-free
# deb-src http://ftp.debian.org/debian unstable main contrib non-free' | sudo tee /etc/apt/sources.list.d/debian-unstable-for-scala.list
  sudo apt-get update
  else
  # Increase apt cache limit, install basic versions of deps
  ssh -t "$1" "echo 'APT::Cache-Limit 50000000;' | sudo tee /etc/apt/apt.conf"
  ssh -t "$1" sudo apt-get update
  ssh -t "$1" sudo aptitude -y install $( cat deps.txt )

  # Enable Debian Unstable, install scala 2.9.1 or later
  ssh -t "$1" "echo 'deb http://ftp.debian.org/debian unstable main contrib non-free
deb-src http://ftp.debian.org/debian unstable main contrib non-free' | sudo tee /etc/apt/sources.list.d/debian-unstable-for-scala.list"
  ssh -t "$1" sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys AED4B06F473041FA
  ssh -t "$1" sudo apt-get update
  ssh -t "$1" sudo apt-get -y install scala

  # Reset apt sources to current distribution
  ssh -t "$1" "echo '# deb http://ftp.debian.org/debian unstable main contrib non-free
# deb-src http://ftp.debian.org/debian unstable main contrib non-free' | sudo tee /etc/apt/sources.list.d/debian-unstable-for-scala.list"
  ssh -t "$1" sudo apt-get update
  fi
}

hdfs () {
  ssh "$1" rm -rf hdfs
  ssh "$1" mkdir -p hdfs/name
  ssh "$1" mkdir -p hdfs/data
  ssh "$1" mkdir -p hdfs/buffer
  # Make sure HDFS is running?
}

selfssh () {
  if [ "$1" == "localhost" ]
  then
  ssh-keygen -t dsa -P "''" -f ~/.ssh/id_"$1"
  ln -s id_"$1" .ssh/id_dsa
  cat ~/.ssh/id_$1.pub >> ~/.ssh/authorized_keys
  else
  ssh "$1" ssh-keygen -t dsa -P "''" -f ~/.ssh/id_"$1"
  ssh "$1" ln -s id_"$1" .ssh/id_dsa
  ssh "$1" "cat ~/.ssh/id_$1.pub >> ~/.ssh/authorized_keys"
  fi
}

for k in $slaves; do
  echo "Preparing $k ..."
  if [ "$1" == "-d" ]; then
    # install deps
    aptstuff "$k"
    hdfs "$k"
#    selfssh "$k"
  fi
  
  ssh "$k" mkdir -p eemil-berkeley/energy-spark
	rsync -az src bin spark jar *sh "$k:$HOME/eemil-berkeley/energy-spark/."
	rsync -az $HOME/mesos $HOME/spark $HOME/hadoop* "$k":

done
