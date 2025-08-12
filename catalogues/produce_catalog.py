import os
import pandas as pd
import numpy as np
import copy
import matplotlib as mpl
import matplotlib.pyplot as plt
import logging
import glob

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
def set_xticks_and_labels(ax, mat, varss, scess):
    ax.set_xticks(np.arange(0, len(mat.columns), len(scess)) + len(scess) / 2)
    ax.set_xticklabels([str(n) for n in varss], fontsize=10)
    ax.xaxis.tick_top()

def set_yticks_and_labels(ax, mat,project):
    ax.set_yticks(np.arange(0.5, len(mat.index) + 0.5))
    if project in ["CMIP5","CORDEX-EUR-11"]:
        ax.set_yticklabels(['_'.join(col.split('_')[:]) for col in mat.index], fontsize=10)
    else:
        ax.set_yticklabels(['_'.join(col.split('_')[:2]) for col in mat.index], fontsize=10)

def add_annotations_and_lines(ax, mat_values, nsim):
    nt = 0
    for dom in nsim.keys():
        ax.annotate(dom, [np.shape(mat_values)[1] + 0.5, nt + (nsim[dom] / 2)],
                    fontsize=90, annotation_clip=False, bbox=dict(boxstyle="round", alpha=0.1))
        ax.plot((0, np.shape(mat_values)[1]), (nt + nsim[dom], nt + nsim[dom]), 'k', linewidth=10)
        nt += nsim[dom]

def add_separators(ax, mat, scess):
    for yv in np.arange(0, len(mat.columns), len(scess)) + len(scess):
        ax.plot((yv, yv), (0, len(mat.index)), 'k', linewidth=10)

def add_colorbar(fig, ax, pl, scess, project):
    if 'CMIP5' in project or 'CMIP6' in project or 'CORDEX' in project:
        cbaxes = fig.add_axes([0.1, 0, 0.2, 0.02])
        cbar = fig.colorbar(pl, orientation='horizontal', cax=cbaxes, ticks=np.arange(0.5, len(scess) + 0.5), pad=0.1)
        cbar.ax.set_xticklabels(scess)
        cbar.ax.tick_params(labelsize=10)
    else:
        # fig.add_axes([left, bottom, width, height]) - [x, y, w, h] as fractions of the figure size
        cbaxes = fig.add_axes([0.25, 0, 0.4, 0.08])
        cbar = fig.colorbar(pl, orientation='horizontal', cax=cbaxes,  ticks=np.arange(1/(len(scess)+1),1,1/(len(scess))), pad=0.1)
        cbar.ax.set_xticklabels(scess)
        print(scess)
        cbar.ax.tick_params(labelsize=5)
def plot2(dataframe,varss, project,scess=['historical', 'rcp26', 'rcp45', 'rcp85'],list_values=['downloaded', 'partial', 'not downloaded']):
    """
    Plot a heatmap based on the given dataframe, project, and variables.

    Parameters:
    - dataframe: pd.DataFrame, the input dataframe.
    - project: str, the project name.
    - variables: list, the list of variables to plot.
    - nsim: int, the number of simulations.
    - results: str, the directory to save the results.
    """

    # Define colormaps and scenarios based on the project
    if 'CMIP5' in project or 'CORDEX-EUR' in project:
        cmap = mpl.colors.ListedColormap(['#C1CDCD', '#1874CD', '#ADD8E6', '#FF8000'])
    elif project == 'CORDEX-CORE':
        cmap = mpl.colors.ListedColormap(['#C1CDCD', '#1874CD', '#FF8000'])
    else:
        cmap = ["#43A055",'#FF8000',"#FF0000"]
    
    mat = dataframe
    mat_values = np.array(mat.values, dtype='float64')

    fig, ax = plt.subplots(figsize=(len(mat.columns), len(mat.index)))


    # Create a new ListedColormap with the selected colors of same lenght as the number of unique values in mat_values
    if len(np.unique(mat_values)) < len(cmap):
        selected_colors = cmap[:len(np.unique(mat_values))]
        
        cmap = mpl.colors.ListedColormap(selected_colors)
        for key, value in load_values_dict().items():
            if value not in mat_values:
                list_values.remove(key) 
    cmap.set_bad(color='w', alpha=1.)
    pl = ax.pcolormesh(mat_values, cmap=cmap, edgecolors='w', linewidths=0.005, vmin=0, vmax=len(scess))

    set_xticks_and_labels(ax, mat, varss, scess)
    set_yticks_and_labels(ax, mat,project)


    add_separators(ax, mat, scess)
    add_colorbar(fig, ax, pl, list_values, project)

    name_file = f"{project}_catalogue"
    plt.savefig(f"{name_file}.pdf", bbox_inches='tight')
    plt.close()


def check_nc_file_for_year(directory, year):
    # Use glob to find .nc files in the directory that contain the specific year
    pattern = os.path.join(directory, f'*{year}*.nc')
    nc_files = glob.glob(pattern)
    return len(nc_files) > 0


def get_earliest_and_latest_dates(directory):
    # Use glob to find all .nc files in the directory
    nc_files = glob.glob(os.path.join(directory, '*.nc'))

    # Extract the last part of the filename after splitting by '_' and remove the extension
    dates = []
    for file in nc_files:
        # Split the filename by '_' and take the last part
        last_part = os.path.basename(file).split('_')[-1]
        # Remove the .nc extension
        year = last_part.replace('.nc', '')
        if year.isdigit():  # Ensure it's a valid year
            dates.append(int(year))

    if not dates:
        return None, None  # Return None if no valid dates are found

    # Determine the earliest and latest dates
    earliest_year = min(dates)
    latest_year = max(dates)

    return earliest_year, latest_year

def create_auxiliar_df(data):
    # Create a list to store rows for the DataFrame
    rows = []

    # Process each row in the CSV file
    for _, row in data.iterrows():
        path = os.path.join(row['path_download'], row['dataset'], row['filename_variable'])

        # Check for each year separately
        start_year_exists = check_nc_file_for_year(path, row['cds_years_start'])
        end_year_exists = check_nc_file_for_year(path, row['cds_years_end'])
        earliest_dates, latest_dates = get_earliest_and_latest_dates(path)
        rows.append({
                'variable': row['filename_variable'],
                'dataset': row['dataset'],
                'product_type': row['product_type'],
                'path': path,
                f'start_file_exists': start_year_exists,
                f'final_file_exists': end_year_exists,
                'earliest_date': earliest_dates,
                'latest_date': latest_dates
            })

    # Create a DataFrame from the list of rows
    df = pd.DataFrame(rows)
    return df

def load_values_dict():
    # Create a dictionary to store the values
    values_dict = {}
    values_dict["downloaded"]=0
    values_dict["not_downloaded"]=2
    values_dict["partial"]=1
    return values_dict

def process_csv_file(file_path):
    # Read the CSV file
    data = pd.read_csv(file_path)
    aux_df=create_auxiliar_df(data)
    print(aux_df)
    project = os.path.basename(file_path).split('.')[0]

    if 'experiment' not in aux_df.columns:
        scess=["historical"]
    else:
        scess = aux_df['experiment'].unique()
    varss = aux_df["variable"].unique()
    if 'model' not in aux_df.columns:
        simss = [project]
    # Create a DataFrame with MultiIndex columns for each
    columns = pd.MultiIndex.from_tuples([(var, sce) for var in varss for sce in scess])
    df_final = pd.DataFrame(index=simss, columns=columns)

    for ind in df_final.index:
        for col in df_final.columns:
                if 'experiment' not in aux_df.columns:
                    if aux_df.loc[aux_df['variable'] == col[0]]['start_file_exists'].squeeze() == True and aux_df.loc[aux_df['variable'] == col[0]]['final_file_exists'].squeeze() == True:
                            value=0
                    elif aux_df.loc[aux_df['variable'] == col[0]]['earliest_date'].squeeze() is not None:
                            value=1
                    else:
                        value=2

                df_final.loc[ind][col] = value
    aux_df.to_csv(f"{project}_catalogue.csv", index=False)
    plot2(df_final,varss, project,scess,list_values=list(load_values_dict().keys()))
    # Print the DataFrame

def main():
    #1-Process the request csv files
    csv_directory = '../requests'
    for filename in os.listdir(csv_directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(csv_directory, filename)
            process_csv_file(file_path)

    #2-Concatenate all the auxiliary csv files
    current_folder = os.getcwd()
    dataframes = []
    for filename in os.listdir(current_folder):
        if filename.endswith('.csv'):
            if filename == "all_catalogues.csv":
                continue
            file_path = os.path.join(current_folder, filename)
            df = pd.read_csv(file_path, index_col=None)
            dataframes.append(df)

    #3-Final catalogue
    if dataframes:
        concatenated_df = pd.concat(dataframes, ignore_index=True)
        print("Concatenated DataFrame:")

        concatenated_df.to_csv("all_catalogues.csv")
if __name__ == "__main__":
    main()