import os
import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import logging
import glob
import sys
sys.path.append('../utilities')
from utils import build_output_path, load_output_path_from_row

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Crear carpetas si no existen (in the catalogues directory at root level)
os.makedirs("../../catalogues/catalogues", exist_ok=True)
os.makedirs("../../catalogues/images", exist_ok=True)

# ---------------- Funciones de visualización ----------------
def set_xticks_and_labels(ax, mat, varss, scess):
    ax.set_xticks(np.arange(0, len(mat.columns), len(scess)) + len(scess)/2)
    ax.set_xticklabels([str(n) for n in varss], fontsize=10)
    ax.xaxis.tick_top()

def set_yticks_and_labels(ax, mat, project):
    ax.set_yticks(np.arange(0.5, len(mat.index)+0.5))
    if project in ["CMIP5", "CORDEX-EUR-11"]:
        ax.set_yticklabels(['_'.join(col.split('_')[:]) for col in mat.index], fontsize=10)
    else:
        ax.set_yticklabels(['_'.join(col.split('_')[:2]) for col in mat.index], fontsize=10)

def add_separators(ax, mat, scess):
    for yv in np.arange(0, len(mat.columns), len(scess)) + len(scess):
        ax.plot((yv, yv), (0, len(mat.index)), 'k', linewidth=10)

def add_colorbar(fig, ax, pl, scess, project):
    if 'CMIP5' in project or 'CMIP6' in project or 'CORDEX' in project:
        cbaxes = fig.add_axes([0.1, 0, 0.2, 0.02])
        cbar = fig.colorbar(pl, orientation='horizontal', cax=cbaxes,
                            ticks=np.arange(0.5, len(scess)+0.5), pad=0.1)
        cbar.ax.set_xticklabels(scess)
        cbar.ax.tick_params(labelsize=10)
    else:
        cbaxes = fig.add_axes([0.25, 0, 0.4, 0.08])
        cbar = fig.colorbar(pl, orientation='horizontal', cax=cbaxes,
                            ticks=np.arange(1/(len(scess)+1), 1, 1/(len(scess))), pad=0.1)
        cbar.ax.set_xticklabels(scess)
        cbar.ax.tick_params(labelsize=5)

def plot2(dataframe, varss, project, scess=['historical', 'rcp26', 'rcp45', 'rcp85'],
          list_values=['downloaded', 'partial', 'not downloaded']):
    # Colormap según proyecto
    if 'CMIP5' in project or 'CORDEX-EUR' in project:
        cmap = mpl.colors.ListedColormap(['#C1CDCD', '#1874CD', '#ADD8E6', '#FF8000'])
    elif project == 'CORDEX-CORE':
        cmap = mpl.colors.ListedColormap(['#C1CDCD', '#1874CD', '#FF8000'])
    else:
        cmap = ["#43A055", "#FF8000", "#FF0000"]

    mat = dataframe
    mat_values = np.array(mat.values, dtype='float64')

    fig, ax = plt.subplots(figsize=(len(mat.columns), len(mat.index)))

    if len(np.unique(mat_values)) < len(cmap):
        selected_colors = cmap[:len(np.unique(mat_values))]
        cmap = mpl.colors.ListedColormap(selected_colors)
        for key, value in load_values_dict().items():
            if value not in mat_values:
                list_values.remove(key)
    cmap.set_bad(color='w', alpha=1.)
    pl = ax.pcolormesh(mat_values, cmap=cmap, edgecolors='w', linewidths=0.005, vmin=0, vmax=len(scess))

    set_xticks_and_labels(ax, mat, varss, scess)
    set_yticks_and_labels(ax, mat, project)
    add_separators(ax, mat, scess)
    add_colorbar(fig, ax, pl, list_values, project)

    # Guardar imagen en carpeta images
    name_file = f"{project}_catalogue"
    plt.savefig(f"../../catalogues/images/{name_file}.png", bbox_inches='tight', dpi=300)
    plt.close()

# ---------------- Funciones de procesamiento ----------------
def check_nc_file_for_year(directory, year):
    pattern = os.path.join(directory, f'*{year}*.nc')
    return len(glob.glob(pattern)) > 0

def get_earliest_and_latest_dates(directory):
    nc_files = glob.glob(os.path.join(directory, '*.nc'))
    dates = []
    for file in nc_files:
        last_part = os.path.basename(file).split('_')[-1].replace('.nc','')
        if last_part.isdigit():
            dates.append(int(last_part))
    if not dates:
        return None, None
    return min(dates), max(dates)

def create_auxiliar_df(data):
    rows = []
    for _, row in data.iterrows():
        # Use utility function to build data_path
        data_path = str(load_output_path_from_row(row))
        
        # Build origin_path
        if row['input_path'] == "CDS":
            origin_path = "CDS"
        else:
            # For derived data, origin is typically raw data with same temporal resolution
            if row['product_type'] == 'derived':
                origin_path = str(build_output_path(
                    row['input_path'],
                    row['dataset'],
                    'raw',
                    row['temporal_resolution'],
                    'native',  # Origin is typically native (non-interpolated)
                    row['filename_variable']
                ))
            else:
                # For raw data, origin is the same as data_path
                origin_path = data_path
        
        start_year_exists = check_nc_file_for_year(data_path, row['cds_years_start'])
        end_year_exists = check_nc_file_for_year(data_path, row['cds_years_end'])
        earliest_dates, latest_dates = get_earliest_and_latest_dates(data_path)

        new_row = {
            'variable': row['filename_variable'],
            'model': 'None',
            'experiment': 'None',
            'dataset': row['dataset'],
            'dataset_type': row['dataset_type'],
            'product_type': row['product_type'],
            'temporal_resolution': row['temporal_resolution'],
            'interpolation': row['interpolation'],
            'data_path': data_path,
            'origin_path': origin_path,
            'start_file_exists': start_year_exists,
            'final_file_exists': end_year_exists,
            'earliest_date': earliest_dates,
            'latest_date': latest_dates,
            'script': row['script']
        }

        if row['dataset_type'] in ['projections']:
            new_row['model'] = row['model']
            new_row['experiment'] = row['experiment']

        rows.append(new_row)

    df = pd.DataFrame(rows, columns=[
        'variable','model','experiment','dataset','dataset_type',
        'product_type','temporal_resolution','interpolation','data_path','origin_path',
        'start_file_exists','final_file_exists','earliest_date','latest_date','script'
    ])
    return df, row['dataset_type']

def load_values_dict():
    return {"downloaded":0,"not_downloaded":2,"partial":1}

def process_csv_file(file_path, type_data):
    data = pd.read_csv(file_path)
    data = data[data['product_type']==type_data]
    if data.empty:
        return
    aux_df, dataset_type = create_auxiliar_df(data)
    project = os.path.basename(file_path).split('.')[0]

    if dataset_type in ["reanalysis"]:
        simss = [project]
        scess = ["historical"]
    else:
        scess = aux_df['experiment'].unique()
    varss = aux_df["variable"].unique()

    columns = pd.MultiIndex.from_tuples([(var, sce) for var in varss for sce in scess])
    df_final = pd.DataFrame(index=simss, columns=columns)

    for ind in df_final.index:
        for col in df_final.columns:
            if dataset_type in ["reanalysis"]:
                logging.info(f"Processing variable {col[0]} for reanalysis dataset {project}")
                logging.info(f"aux_df: {aux_df}")
                if aux_df.loc[aux_df['variable']==col[0]]['start_file_exists'].squeeze()==True and \
                   aux_df.loc[aux_df['variable']==col[0]]['final_file_exists'].squeeze()==True:
                    value=0
                elif aux_df.loc[aux_df['variable']==col[0]]['earliest_date'].squeeze() is not None:
                    value=1
                else:
                    value=2
            df_final.loc[ind,col] = value

    # Guardar CSV en carpeta catalogues
    aux_df.to_csv(f"../../catalogues/catalogues/{project}_{type_data}_catalogue.csv", index=False)
    # Generar imagen de las descargas
    if type_data == "raw":
        plot2(df_final, varss, project, scess, list_values=list(load_values_dict().keys()))

# ---------------- Main ----------------
def main():
    # Note: Interpolated data is now stored as 'derived' with non-native interpolation
    type_data_list = ["raw","derived"]
    csv_directory = '../../requests'

    # Procesar CSVs individuales
    for type_data in type_data_list:
        for filename in os.listdir(csv_directory):
            if filename.endswith('.csv'):
                file_path = os.path.join(csv_directory, filename)
                process_csv_file(file_path, type_data)

    # Concatenar todos los CSVs auxiliares
    catalogue_folder = "../../catalogues/catalogues"
    dataframes = []
    for filename in os.listdir(catalogue_folder):
        if filename.endswith('.csv') and filename != "all_catalogues.csv":
            file_path = os.path.join(catalogue_folder, filename)
            df = pd.read_csv(file_path, index_col=None)
            df['earliest_date'] = pd.to_numeric(df['earliest_date'], errors='coerce').astype('Int64')
            df['latest_date'] = pd.to_numeric(df['latest_date'], errors='coerce').astype('Int64')
            dataframes.append(df)

    if dataframes:
        concatenated_df = pd.concat(dataframes, ignore_index=True)
        concatenated_df.to_csv(os.path.join(catalogue_folder, "all_catalogues.csv"), index=False)
        logging.info(f"All catalogues saved in {os.path.join(catalogue_folder, 'all_catalogues.csv')}")

if __name__ == "__main__":
    main()
