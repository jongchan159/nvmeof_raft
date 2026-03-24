test this while server running

sudo rm -f /mnt/nvmeof_raft/metadata*/md_*.dat

go build -o mdparser ./cmd/mdparse


# eternity4 (리더):
sudo ./mdparser --device=/dev/nvme1n1 --pba=0x208B08200

# eternity5 (팔로워):
sudo ./mdparser --device=/dev/nvme2n1 --pba=0x488B0C200
sudo dd if=/dev/nvme2n1 bs=512 skip=$((0x488D00200/512)) count=1 2>/dev/null | xxd | head -3

# eternity6 (팔로워):
sudo ./mdparser --device=/dev/nvme0n1 --pba=0x68870C200