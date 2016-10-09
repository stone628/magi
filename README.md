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
* minibian on Raspberry Pi 2
  * install prerequisites
   ```
sudo apt-get install build-essentials luajit luarocks cmake
   ```
  * install dependent luarocks
   ```
sudo luarocks install luv
   ```
  
