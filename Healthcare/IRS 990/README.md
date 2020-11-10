# IRS 990

This task was as interesting as it was difficult. We needed to parse hundreds of thousands of IRS 990 tax returns for healthcare organizations.
There were a few difficulties: 
1. **The data is in XML format** and at the time I had little experience with XML data.
2. **The data is complex.** It links organizations to their parent organizations, child organizations, tax-liable entities and contains information about the transactions between organizations, etc.
3. **The data is inconsistent.** The icing on the cake of course, was that after I had explored the structure of the data and identified where our key fields were located, things changed.
It turns out that the structure of the data, and in particular the names of several important key fields, changed depending on how the information was collected.

This was a great exercise in exploring complex datasets and dealing with multiple formats and data types in the data collection process. 
I also stumbled upon a lot of work from other researchers which helped me along the way (this is mentioned once or twice in the comments)

- [`read_irs`](https://github.com/MokeEire/PolicyWork/blob/main/Healthcare/IRS%20990/read_irs.R) and [`irs_dataframe`](https://github.com/MokeEire/PolicyWork/blob/main/Healthcare/IRS%20990/irs_dataframe.R)
deal mostly with reading the data into R
- [`master_concordance_xpaths`](https://github.com/MokeEire/PolicyWork/blob/main/Healthcare/IRS%20990/master_concordance_xpaths.R) makes use of [Open Data Collective's Master Concordance File](https://nonprofit-open-data-collective.github.io/irs-efile-master-concordance-file/)
to produce a list of all the possible xpaths for the variables we are interested in
