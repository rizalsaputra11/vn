ips = [f"10.0.2.{i}" for i in range(2, 255)]
with open("ips.txt", "w") as f:
    f.write("\n".join(ips))
print(f"Wrote {len(ips)} IPs to ips.txt")

