# -*- mode: ruby -*-
# vi: set ft=ruby :

go_path = "/home/vagrant/go"
src_dir = "#{go_path}/src/github.com/hatelikeme/storage"

Vagrant.configure("2") do |config|
  config.vm.box = "geerlingguy/centos7"

  config.vm.network :forwarded_port, host: 8000, guest: 8000

  config.vm.provider "virtualbox" do |vb|
     vb.memory = "4096"
  end

  config.vm.synced_folder '.', '/vagrant', disabled: true
  config.vm.synced_folder '.', src_dir, :mount_options => ['dmode=775','fmode=664']

  config.vm.provision "ansible" do |ansible|
    ansible.playbook = "ansible/main.yml"
  end

  config.vm.provision :shell, :inline => "sudo chmod -R 777 #{go_path}"

end
