These scripts are for supporting a CTPA repeat audit

The CTReject.Rmd markdown file is designed for use with the MIDASh R package, which is currently closed source.

Running this markdown file will require you to set up a direct SQL connection to an instance of OpenREM.

On first running the markdown file, it should generate 2 csv files but fail.

First, manually edit 'ctpa_varieties.csv'.
Remove any rows you'd like to exclude from analysis.
Overwrite the 'group_name' column if there are certain types of procedure you feel ought to be bundled together.

Then, run the other scripts.
Running these scripts will require you to have access to an instance of Orthanc and direct query access to your PACS.
Data is moved to Orthanc from PACS, and dicom files are retrieved directly from Orthanc using the REST interface.
The details of this are initialised in the medphunc python package: https://github.com/ckswilliams/medphunc.

After running both python scripts, 4 new csv files should appear.

Now, rerun the .Rmd file. all things being equal, it should be able to produce a report.

Please raise questions or issues via github Issues.
