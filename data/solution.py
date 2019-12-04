from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import udf
from pyspark.sql import Window
from geopy.distance import geodesic
from math import pi


def get_data():
    """
    Get DataSample and POIList data

    :return: DataFrames representing data found in DataSample and POIList csv files
    """
    request = spark.read.csv(
        '/tmp/data/DataSample.csv',
        header=True,
        inferSchema=True,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True
    )
    poi = spark.read.csv(
        '/tmp/data/POIList.csv',
        header=True,
        inferSchema=True,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True
    )
    return request, poi


def cleanup(request):
    """
    Clean up suspicious request records by removing those with duplicate TimeSt, Latitude, and Longitude

    :param request: Dataframe containing request records
    :return: Updated request Dataframe containing all non-suspicious request records
    """
    request = request.drop_duplicates(subset=['TimeSt', 'Latitude', 'Longitude'])
    return request


def distance(lat1, long1, lat2, long2):
    """
    Calculate distance between two points given by their latitudes and longitudes

    :param lat1: Latitude for point 1
    :param long1: Longitude for point 1
    :param lat2: Latitude for point 2
    :param long2: Longitude for point 2
    :return: The distance between point 1 and point 2 in kilometers
    """
    point1 = (lat1, long1)
    point2 = (lat2, long2)
    distance = geodesic(point1, point2).kilometers
    return distance


def label(request, poi):
    """
    Label each request record with the closest POI. Should a request record have multiple POIs that are equidistant
    from it, all such POIs will be assigned to it.

    :param request: Dataframe containing request records
    :param poi: Dataframe containing POI records
    :return: Dataframe containing request records with their assigned POI information
        Schema (ReqID, Req_Latitude, Req_Longitude, POIID, POI_Latitude, POI_Longitude, Distance)
    """

    # Cross join the request and POI tables
    cross = request.selectExpr('_ID AS ReqID', 'Latitude AS Req_Latitude', 'Longitude AS Req_Longitude') \
        .crossJoin(poi.selectExpr('POIID', 'Latitude AS POI_Latitude', 'Longitude AS POI_Longitude'))

    # Calculate distance from each request to each POI
    distance_udf = udf(distance)
    cross = cross.withColumn("Distance",
                             distance_udf(cross.Req_Latitude, cross.Req_Longitude,
                                          cross.POI_Latitude, cross.POI_Longitude))

    # Get min distance for each request
    w = Window.partitionBy(cross.ReqID)
    labeled = cross.withColumn('MinDistance', f.min(cross.Distance.cast('double')).over(w)) \
        .where(f.col('MinDistance') == f.col('Distance')) \
        .drop(f.col('MinDistance'))

    return labeled


def label2(request, poi):
    """
    Label each request record with the closest POI. Should a request record have multiple POIs that are equidistant
    from it, all such POIs will be assigned to it.

    :param request: Dataframe containing request records
    :param poi: Dataframe containing POI records
    :return: Dataframe containing request records with their assigned POI information
        Schema (ReqID, Req_Latitude, Req_Longitude, POIID, POI_Latitude, POI_Longitude, Distance)
    """

    # Cross join the request and POI tables
    cross = request.selectExpr('_ID AS ReqID', 'Latitude AS Req_Latitude', 'Longitude AS Req_Longitude') \
        .crossJoin(poi.selectExpr('POIID', 'Latitude AS POI_Latitude', 'Longitude AS POI_Longitude'))

    # Calculate distance from each request to each POI
    distance_udf = udf(distance)
    cross = cross.withColumn("Distance",
                             distance_udf(cross.Req_Latitude, cross.Req_Longitude,
                                          cross.POI_Latitude, cross.POI_Longitude))

    # Get min distance for each request
    min_dist = cross.groupBy(cross.ReqID.alias('RID')).agg(f.min(cross.Distance.cast('double')).alias('MinDistance'))

    # Join back to only keep min distance rows
    cross = cross.join(min_dist, [cross.ReqID == min_dist.RID, cross.Distance == min_dist.MinDistance]) \
        .drop(f.col('MinDistance')).drop(f.col('RID'))

    return cross


def density(radius, count):
    """
    Calculate the density of a circle given its radius and count

    :param radius: The radius of the circle
    :param count: The count of how many things are inside the circle
    :return: The count density of the circle
    """
    area = pi * radius**2
    density = count / area
    return density


def analysis(labeled):
    """
    Perform calculations on labeled data

    :param labeled: Dataframe containing request records with their assigned POI information
    :return: Dataframe containing aggregate data calculations for each POI
        Schema (POIID, Average, Std_Deviation, Radius, Count, Density)
    """
    # Calculate average and standard deviation for each POI
    analysis = labeled.groupBy(labeled.POIID) \
        .agg(f.avg(labeled.Distance).alias('Average'),
             f.stddev(labeled.Distance).alias('Std_Deviation'),
             f.max(labeled.Distance.cast('double')).alias('Radius'),
             f.count(labeled.Distance).alias('Count'))

    density_udf = udf(density)
    analysis = analysis.withColumn('Density', f.round(density_udf(analysis.Radius, analysis.Count), 10))

    return analysis


def popularity(avg, std_dev, num_req, total_req):
    """
    Calculates the popularity of a POI given some of its assigned request statistics

    :param avg: Average distance away from the POI of all its assigned requests
    :param std_dev: Standard deviation of the above average
    :param num_req: Number of requests assigned to this POI
    :param total_req: Total number of requests assigned to any POI
    :return: A popularity ranking value between -10 and 10
    """
    x = 2 * (1 / pow(2, avg/std_dev)) - 2 * (1 - (num_req/total_req))
    sigmoid = x / pow(1 + pow(x, 2), 0.5)
    popularity = 10 * sigmoid

    return popularity


def model(analysis, total_req):
    """
    Map each POI with a popularity value

    :param analysis: Dataframe containing aggregate data calculations for each POI
    :param total_req: Total number of requests assigned to any POI
    :return: Dataframe of analysis with an additional Popularity column
        Schema (POIID, Average, Std_Deviation, Radius, Count, Density, Popularity)
    """
    model = analysis.withColumn('Popularity',
                                popularity(analysis.Average, analysis.Std_Deviation, analysis.Count, total_req))
    return model


if __name__ == "__main__":
    spark = SparkSession.builder.appName("EQ_Works_Solution").getOrCreate()

    request, poi = get_data()

    # Problem 1: Cleanup
    request = cleanup(request)

    # Problem 2: Label
    labeled = label(request, poi)

    # Problem 3: Analysis
    analysis = analysis(labeled)

    # Problem 4a: Model
    # See solution.md for an explanation of the math model chosen for this problem
    total_requests = request.count()
    model = model(analysis, total_requests)
    model.show()

    spark.stop()
