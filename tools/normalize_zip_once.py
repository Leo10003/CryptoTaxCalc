
import zipfile, pathlib, shutil, os, tempfile

def normalize_zip(zip_path: str):
    z = pathlib.Path(zip_path)
    tmp = z.with_suffix(".normalized.zip")
    seen = set()
    # We’ll keep the last entry for each name by iterating forward and overwriting in-memory
    with zipfile.ZipFile(z, "r") as zin, zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        # Build a map of last occurrence
        last_index = {}
        for i, info in enumerate(zin.infolist()):
            last_index[info.filename] = i
        for i, info in enumerate(zin.infolist()):
            if last_index[info.filename] != i:
                continue  # skip duplicates, keep only last
            data = zin.read(info.filename)
            zout.writestr(info, data)
    backup = z.with_suffix(".bak.zip")
    shutil.move(z, backup)
    shutil.move(tmp, z)
    print(f"Normalized {z.name}. Backup at {backup.name}")

if __name__ == "__main__":
    normalize_zip("support_bundles/''SUPPORT_BUNDLE''.zip") #edit file name here
