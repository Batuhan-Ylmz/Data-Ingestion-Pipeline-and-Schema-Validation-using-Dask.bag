# Data-Ingestion-Pipeline-and-Schema-Validation-using-Dask.bag
A +2 GB .txt file was read using different big data processing libraries of python.
Such as Modin, Ray, Pandas, Dask, Dask.dataframe, Dask.Bag.
Methods were compared based on their reading computation times ( for the specific .txt file).

Schema validation was provided with a utility file based on a YAML file. (file.YAML in repo). 
After validation and data summary informations, data was written with the limitations ( outbound delimiter) in the YAML file.

NOTE: Due to the insufficient amount of memory of local PC, checkpoint command and pattern validation for paragraphs were commented.
Can be activated for PCs with sufficient amount of memories to write the file in any intended format ( .gz, .tar. zip .. etc).
