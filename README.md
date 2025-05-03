# cs532-final-project

## Usage

We developed on IntelliJ Community Edition and thus are providing the instructions for the setup we used to run the programs on IntelliJ. 

There are two main Flink Jobs in the project. The first is the `ProjectAnalysisJob.java` in the cms1D directory. This is the code for the CMS prototype mentioned in the presentation. In order to run this job, right click on the file in IntelliJ and click the `Run PurchaseAnalysisJob main()` option. If this doesn't work go to the configurations and ensure 
    
    Add dependencies with "provided" scope to classpath 

is selected. Also enable the `Add VM Options` and add the following to the field created

    --add-opens=java.base/java.util=ALL-UNNAMED

The other Flink job is also called `ProjectAnalysisJob.java` but is in the `cms2D` directory. This version contains the fixes for the problems we identified in the prototype. The instructions for running this job are the same as the previous job.

## Design Description

The [Amazon Sales Dataset](https://www.kaggle.com/datasets/karkavelrajaj/amazon-sales-dataset) contains ~1,400 rows of item purchases from Amazon. To construct an unbounded stream from this data, we created a rate-limited data source which generates random purchases from a distribution over item categories. This distribution can be tuned by specifying weights for each category in `resources/weights.yaml`. If the total weight of these specified categories is less than 100%, the remaining weight is uniformly distributed over unspecified categories. The dataset itself is stored as a CSV file, and the logic to parse it leverages [OpenCSV](https://opencsv.sourceforge.net/) and resides in `stream/PurchaseGenerator`. 

To estimate frequent items in the stream, we implemented the [count-min sketch](https://dsf.berkeley.edu/cs286/papers/countmin-latin2004.pdf) (CMS) data structure in cms2D/Sketch. CMS uses sublinear space but introduces an estimation error. Each `Sketch` encapsulates a 2D array of estimates. Each row in the array is associated with a `MurmurHash3.hash32x86` hash function. When an item arrives, its category is passed through each hash function, and the hashes are used to increment the counters of the corresponding rows. Notice counters never underestimate with this scheme, so the minimum counter is the best estimate for a given category’s frequency.

To achieve a distributed CMS, incoming items are randomly distributed across `NUM_CORES` workers using a `RandomKeySelector`. Workers run the `WindowCMS` process, which creates and updates a local `Sketch` object. After a set time interval, the workers submit their local sketches to a global coordinator running the `Merger` process. Sketches are merged by simply adding corresponding entries. 

Assuming the number of categories can be arbitrarily large, it would be costly to extract estimates for every category from a merged CMS. Instead, our solution has each worker maintain a size-limited `TreeSet`, which stores the top `MAX_HOT_KEYS` categories with the largest local estimates in `HotKey` objects. `TreeSet` was chosen since it supports item insertion and deletion in logarithmic time. Workers emit these categories along with their sketches. The coordinator then uses this reduced set on the merged CMS to determine frequent categories, significantly improving performance. This works because purchases are distributed randomly across the workers, which preserves relative frequencies in expectation. For instance, if there are 5 workers and a stream of 75 printers and 25 webcams,  each worker is expected to receive 15 printers and 5 webcams.  Thus, if printers are globally popular, they’re expected to be locally popular and likely represented in a HotKey emitted by a worker
