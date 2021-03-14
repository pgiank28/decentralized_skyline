# Decentralized skyline algorithm
This is an implementation for Apache Hadoop of the skyline algorithm.It supports input of buildings points in both 2 and 3 dimensions.

### The skyline algorithm
A city's skyline is the outer contour of the silhouette formed by all the buildings in that city when viewed from a distance. Given the locations and heights of all the buildings, the algorithm returns the skyline formed by these buildings collectively.
##### Example
![merged](https://user-images.githubusercontent.com/17511966/111064742-6babb580-84be-11eb-9bc0-d2bc1dc95e76.jpg)

```Input: buildings = [[2,9,10],[3,7,15],[5,12,12],[15,20,10],[19,24,8]]```\
```output: skyline_points = [[2,10],[3,15],[7,12],[12,0],[15,10],[20,8],[24,0]]```


### Implementation in map-reduce model
The algorithm takes as input tuples of 2 or 3 numbers that represent the dimensions of the initial skylines.Every Mapper,computes the skyline of the buildings that it is assigned to,following a traditional appoach(see example).
The reducer than,combines the results,by taking the boundary tuples of all the Mappers combined.It selects the highest,more left and more right points of the Mappers and calculates the area,to output the general skyline of the overall dataset 
