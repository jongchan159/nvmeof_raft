test this while server running

sudo rm -f /mnt/nvmeof_raft/metadata*/md_*.dat

go build -o mdparse ./cmd/mdparse


# eternity4 (리더):
sudo ./mdparse --device=/dev/nvme1n1 --pba=0x208900400

# eternity5 (팔로워):
sudo ./mdparse --device=/dev/nvme2n1 --pba=0x488900200

# eternity6 (팔로워):
sudo ./mdparse --device=/dev/nvme0n1 --pba=0x588B00200