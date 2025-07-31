def tp(ds, var)
    """
    Convert daily accumulated precipitation (mm/day) to precipitation flux (kg/m²/s) and update the attributes accordingly.
    """
    for var in ds.data_vars:
    	if var in ds.var:
    	    ds[var] = ds[var] - 273.15
            ds[var].attrs['standard_name'] = "precipitation_flux"
            ds[var].attrs['units'] = "kg m-2 s-1"
            ds[var].attrs['cell_methods'] = "area: time: mean"
            ds[var].attrs['long_name'] = "Precipitation"
            ds[var].attrs['comment'] = "includes both liquid and solid phases"    		
            ds[var] = ds[var]/86400
    return ds



