# magi
game server powered by luajit

## Goal
* suited for multi-player game, PC and mobile alike
* provide environment where single-threaded server contents can outperform complex multi-threaded codes

## To Do
* make simple echo server/client
* make session queue for message process
* make sessions transfer from one session queue to another

## Build Environment
### minibian on Raspberry Pi
#### install prerequisites
   ```
$ sudo apt-get install build-essentials luajit luarocks cmake
   ```
#### install dependent luarocks
   ```
$ sudo luarocks install luv lua-messagepack
   ```
#### install docker on rpi - from https://github.com/umiddelb/armhf/wiki/Get-Docker-up-and-running-on-the-RaspberryPi-(ARMv6)-in-four-steps-(Wheezy)
   ```
$ curl -ks https://packagecloud.io/install/repositories/Hypriot/Schatzkiste/script.deb.sh | sudo bash
$ sudo apt-get install docker-hypriot=1.10.3-1
$ sudo usermod -aG docker ${USERNAME}
$ sudo systemctl enable docker.service
   ```
#### docker recipes - redis
   ```
$ docker run -d -p 6379:6379 hypriot/rpi-redis
   ```
