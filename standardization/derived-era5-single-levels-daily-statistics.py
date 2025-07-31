def tp_mm(ds, var):
    """
    Convert daily accumulated precipitation (mm/day) to precipitation flux (kg/m²/s) and update the attributes accordingly.
    """
    for data_var in ds.data_vars:
    	if var == data_var:
            ds[var].attrs['standard_name'] = "precipitation_flux"
            ds[var].attrs['units'] = "kg m-2 s-1"
            ds[var].attrs['cell_methods'] = "area: time: sum"
            ds[var].attrs['long_name'] = "Precipitation"
            ds[var].attrs['comment'] = "includes both liquid and solid phases"    		
            ds[var] = ds[var]/86400
    return ds


def tp_m(ds, var):
    """
    Convert daily accumulated precipitation (m of water equivalent/day) to precipitation flux (kg/m²/s) and update the attributes accordingly.
    """
    for data_var in ds.data_vars:
    	if var == data_var:
            ds[var].attrs['standard_name'] = "precipitation_flux"
            ds[var].attrs['units'] = "kg m-2 s-1"
            ds[var].attrs['cell_methods'] = "area: time: sum"
            ds[var].attrs['long_name'] = "Precipitation"
            ds[var].attrs['comment'] = "includes both liquid and solid phases"    		
            ds[var] = ds[var]*1000/86400
    return ds



def e_m(ds, var):
    """
    Convert daily accumulated evaporation (mm/day) to evaporation flux (kg/m²/s) and update the attributes accordingly.
    """
    for data_var in ds.data_vars:
    	if var == data_var:
            ds[var].attrs['standard_name'] = "surface_water_evaporation_flux"
            ds[var].attrs['units'] = "kg m-2 s-1"
            ds[var].attrs['cell_methods'] = "area: time: sum"
            ds[var].attrs['long_name'] = "Evaporation"
            ds[var].attrs['comment'] = "negative values indicate evaporation and positive values indicate condensation"    		
            ds[var] = ds[var]*1000/86400
    return ds


def ssrd(ds, var):
    """
    Convert daily accumulated surface_solar_radiation_downwards (J m**-2") to (W m-2") and update the attributes accordingly.
    """
    for data_var in ds.data_vars:
    	if var == data_var:
            ds[var].attrs['standard_name'] = "surface_downwelling_shortwave_flux_in_air"
            ds[var].attrs['units'] = "J m-2"
            ds[var].attrs['cell_methods'] = "area: time: sum"
            ds[var].attrs['long_name'] = "Surface short-wave (solar) radiation downwards"  		
            ds[var] = ds[var]/ 3600
    return ds


