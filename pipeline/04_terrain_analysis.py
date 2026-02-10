import rasterio
import numpy as np

with rasterio.open("../data_raw/dem.tif") as src:
    dem = src.read(1)
    profile = src.profile

slope = np.sqrt(
    np.gradient(dem)[0]**2 + np.gradient(dem)[1]**2
)

profile.update(dtype=rasterio.float32)

with rasterio.open("../data_processed/slope.tif", "w", **profile) as dst:
    dst.write(slope.astype(rasterio.float32), 1)
