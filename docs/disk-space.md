# Disk space (Hyper-V Ubuntu VM)

Video ingest/processing can fill this VM quickly (raw `.ts` files are multi‑GB). This doc covers:

- Finding what’s using space
- Safe cleanups (project + user caches)
- Safe Hyper‑V disk expansion (LVM + ext4)

## Quick checks

```bash
df -hT
lsblk -f
du -sh /home/canteenhub/amherst-display-repo/* | sort -h | tail -n 30
find /home/canteenhub/amherst-display-repo -type f -size +1G -printf '%s\t%p\n' | sort -n | tail -n 20
```

## Safe cleanups (no sudo)

### 1) Clear leftover local ingest downloads

The Drive ingest script downloads to `temp/drive_ingest/incoming/`.

If you see giant videos stuck there, you can free space immediately by truncating them:

```bash
truncate -s 0 /home/canteenhub/amherst-display-repo/temp/drive_ingest/incoming/*.ts 2>/dev/null || true
```

If you prefer to delete them entirely, do so (they are just local copies; originals live on Drive).

### 2) Purge pip cache

```bash
/home/canteenhub/amherst-display-repo/venv/bin/pip cache purge
```

### 3) Purge npm cache

```bash
npm cache clean --force
```

## System cleanups (sudo)

Journald can grow large:

```bash
sudo journalctl --disk-usage
sudo journalctl --vacuum-size=500M
```

## Expand the VM disk (Hyper‑V + LVM + ext4)

This VM uses LVM (`/dev/mapper/ubuntu--vg-ubuntu--lv`) on `ext4`. Expanding is routine and non-destructive when done correctly.

### A) Hyper‑V (host)

1. Shut down the VM (don’t “Save state”).
2. Hyper‑V Manager → VM → **Settings** → **Hard Drive** → **Edit…** → **Expand**.
3. Start the VM.

### B) Ubuntu (guest)

Verify the disk got bigger:

```bash
lsblk
```

Grow the partition that backs the LVM PV (commonly `/dev/sda3`):

```bash
sudo apt-get update
sudo apt-get install -y cloud-guest-utils
sudo growpart /dev/sda 3
```

Resize the LVM physical volume, then extend the logical volume + filesystem:

```bash
sudo pvresize /dev/sda3
sudo lvextend -l +100%FREE /dev/ubuntu-vg/ubuntu-lv
sudo resize2fs /dev/ubuntu-vg/ubuntu-lv
df -hT /
```

If your root filesystem is not `ext4` (rare here), stop and use the appropriate grow command (e.g. `xfs_growfs` for XFS).

