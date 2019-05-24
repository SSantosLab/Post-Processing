##################################################################
######## Please refer to `README` for the most up-to-date ########
######## information on running Post Processing.          ########
##################################################################


# Post-Processing

“I ought to be thy Adam, but I am rather the fallen angel...”
--Frankenstein; or, The Modern Prometheus (Mary Shelley)


Code for the post-processing, now known as Adam.

To create a cron job for Adam do:
*/10 * * * * . AdamRUOK.sh
This will check if Adam is running every ten minutes and start it if not.

Adam Flow:


Cron job starts AdamRUOK every 10 minutes. AdamRUOK checks to see if Adam is currently running, if not, it starts Adam. When started, Adam runs a generalized diffimg_setup, then gathers an array of all the .ini files in the folder from which it runs. If Adam sees more than one .ini files, it enters a for loop over them, starting first run_postproc, pushing this to background, starting run_checker, pushing this to background, and moving on to the next .ini file. run_postproc has 9 central steps. 

Step -1: Set the Environment. This pulls data from the .ini file, including the season, ligoid, and similar identifying information.

Step 0: Initialize Master List. 

Step 1: Finalize Master List. 

Step 2: Force Photometry. 

Step 3: Host Match. Finds the most likely host galaxy of a given candidate.

Step 4: Make Truth Table.

Step 5: Make Data Files and Candidate Webpages. Creates data files (which were already there???) on the candidate, and creates mini webpages for each with information from the data files, as well as stamps of the difference, search, and template images for it.

Step 6: Make Plots. Makes plots of the ML_Scores for the fakes and RA and DEC maps for ….

Step 7: Make Web Page. Makes master webpage with links to candidate pages and a status page to show whether each step outlined above ran.

run_checker is much simpler. During each of the run_postproc steps, run checker gather information regarding them and saves it to a file, which it continues to update until run_postproc is completed. It the creates a table of the steps’ statuses and a webpage to house the table.

