### Part 4a: Explanation of Math Model
The mathematical model I have chosen to map a POI to its popularity is as follows:

`Popularity = 10*x / sqrt(1 + x^2)`

where

`x = 2*(1 / 2^(A/S)) - 2*(1-R)`

where
```
A = Average distance away from the POI of all its assigned requests
S = Standard deviation of the above average
R = Percentage of requests that are assigned to the POI
```
At its core,`Popularity` is a sigmoid function designed to be bounded between -10 and 10.
The function `x` incorporates the three variables I believed to be the greatest factors
to determining a POI's popularity:
* Average distance assigned requests are from the POI
    * The closer the average distance, the higher the popularity of a POI
* Standard deviation of the average
    * The smaller the standard deviation, the greater the effect the average has on the
    popularity
* Request percentage
    * The higher the request percentage, the higher the popularity of a POI
    
This model aims to provide the following properties:
* The smaller `A` is, the greater the popularity and the bigger `A` is, the lesser the
popularity.
* Changes in `A` have a big affect only if the standard deviation is small. The greater
the standard deviation, the greater our spread of requests are around the average distance.
This means that when we have a higher standard deviation, the value of our average becomes
less meaningful.
    * For example, a very small average with a very small standard deviation implies that
    many of the assigned request points are close to the POI and therefore, it should
    be very popular. However, a the same small average with a large standard deviation
    should be less popular because some of requests are spread a lot more further out.
    * The same logic applies for averages that are large. With a small standard deviation,
    we are saying that most of the points are in fact far away, around the distance of the
    large average. However, a bigger standard deviation in this scenario would mean that while
    there are many points that are far away, there could also be many that are close by
    and therefore, we should not be as certain in declaring this POI unpopular.
* The more percentage of total requests (`R`) a POI has, the more popular it is. This part of 
the model works well with a small number of POIs and scattered requests like we have in
this example. If we were to model the same thing for a dataset with much more POIs and
requests that tended to group around particular POIs, the factor `2*(1-R)` should be
adjusted to provide much less influence on the resulting mapping.

Note: Density was not used in this calculation because it becomes highly skewed in the
event of outliers (particularly those of requests made very far away from all POIs).
Should we take the analysis further to remove outliers, density could further enhance 
this model.

The popularity mappings given by this model are:
```
POI1 & POI2: 1.740821340993886
POI3: -4.66309662429787
POI4: -3.4664303719674234
```