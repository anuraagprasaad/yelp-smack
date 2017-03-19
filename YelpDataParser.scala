import spark.implicits._

val yelpBusinessData = "/opt/yelp/yelp_academic_dataset_business.json"
val yelpReviewData = "/opt/yelp/yelp_academic_dataset_review.json"
val yelpUserData = "/opt/yelp/yelp_academic_dataset_user.json"
val yelpCheckInData = "/opt/yelp/yelp_academic_dataset_checkin.json"
val yelpTipData = "/opt/yelp/yelp_academic_dataset_tip.json"

// ============= Start Processing yelp_academic_dataset_business.json====================

val yelpBusinessJsonData = sc.textFile(yelpBusinessData)
val businessReviewsDF = spark.read.json(yelpBusinessJsonData).cache()
businessReviewsDF.createOrReplaceTempView("businessReviews")

// Print the schema in a tree format
businessReviewsDF.printSchema()

// Show first 10 records from businessReviews
val firstTenRecords = spark.sql("SELECT business_id, name, city, state, stars FROM businessReviews limit 10")
firstTenRecords.show()

// Top 10 businesses with high review counts > 5000
val topTenBusinessWithReviewsGreaterThan5000 = spark.sql("SELECT state, city,name,review_count from businessReviews WHERE review_count > 1000 ORDER BY review_count DESC LIMIT 10")
topTenBusinessWithReviewsGreaterThan5000.show()

// Top 10 states and cities in total number of reviews.
val countReviewsByStateAndCity = spark.sql("SELECT state, city, COUNT(*) as totalReviews FROM businessReviews GROUP BY state, city ORDER BY totalReviews DESC LIMIT 10")
countReviewsByStateAndCity.show()

// Top 10 average number of reviews per stars
val top10AvgReviewRating = spark.sql("SELECT stars,AVG(review_count) as reviewsAvg from businessReviews GROUP BY stars ORDER BY stars DESC")
top10AvgReviewRating.show()

// Let's now use Spark's Data Frame to show it's capabilities
// Lets Explode some of the nested json

// Exploded attributes
val explodedAttributes = businessReviewsDF.select(explode(businessReviewsDF("attributes")))
explodedAttributes.show

// Exploded categories
val explodedCategories = businessReviewsDF.select(explode(businessReviewsDF("categories")))
explodedCategories.show

// Exploded hours
val explodedHours = businessReviewsDF.select(explode(businessReviewsDF("hours")))
explodedHours.show

// Top 10 Restaurants in the listed categories
val restaurantsCategoryCount = spark.sql("SELECT name, SIZE(categories) as categoryCount, categories FROM businessReviews LATERAL VIEW explode(categories) tab AS category WHERE category = 'Restaurants' ORDER BY categoryCount DESC LIMIT 10")
restaurantsCategoryCount.show()

//Top categories used in business reviews.
val topCategoryBusinessReview = spark.sql("SELECT category, COUNT(category) categoryCount FROM (SELECT EXPLODE(categories) category FROM businessReviews ) tempTable GROUP BY category ORDER BY categoryCount DESC LIMIT 10")
topCategoryBusinessReview.show()
// ============= End Processing yelp_academic_dataset_business.json ====================

// ============= Start Processing yelp_academic_dataset_review.json ====================

val yelpReviewJsonData = sc.textFile(yelpReviewData)
val reviewDF = spark.read.json(yelpReviewJsonData).cache()
reviewDF.createOrReplaceTempView("reviews")

// Print the schema in a tree format
reviewDF.printSchema()

// Show first 10 records from reviews
val firstTenReviewRecords = spark.sql("SELECT * FROM reviews limit 10")
firstTenReviewRecords.show()

// ============= End Processing yelp_academic_dataset_business.json ====================

// ============= Start Processing yelp_academic_dataset_user.json ====================

val yelpUserJsonData = sc.textFile(yelpUserData)
val userDF = spark.read.json(yelpUserJsonData).cache()
userDF.createOrReplaceTempView("users")

// Print the schema in a tree format
userDF.printSchema()

// Show first 10 records from reviews
val firstTenUserRecords = spark.sql("SELECT user_id, name, review_count, yelping_since, average_stars FROM users limit 10")
firstTenUserRecords.show()

// ============= End Processing yelp_academic_dataset_user.json ====================

// ============= Start Processing yelp_academic_dataset_checkin.json ====================

val yelpCheckInJsonData = sc.textFile(yelpCheckInData)
val checkInDF = spark.read.json(yelpCheckInJsonData).cache()
checkInDF.createOrReplaceTempView("checkin")

// Print the schema in a tree format
checkInDF.printSchema()

// Show first 10 records from reviews
val firstTenCheckInRecords = spark.sql("SELECT * FROM checkin limit 10")
firstTenCheckInRecords.show()

// ============= End Processing yelp_academic_dataset_checkin.json ====================

// ============= Start Processing yelp_academic_dataset_tip.json ====================

val yelpTipJsonData = sc.textFile(yelpTipData)
val tipDF = spark.read.json(yelpTipJsonData).cache()
tipDF.createOrReplaceTempView("tip")

// Print the schema in a tree format
tipDF.printSchema()

// Show first 10 records from reviews
val firstTenTipRecords = spark.sql("SELECT * FROM tip limit 10")
firstTenTipRecords.show()

// ============= End Processing yelp_academic_dataset_tip.json ====================

// ============= Some Meomory intensive JOIN operations between business and review data ====================

val businessDF = spark.sql("SELECT business_id, name, city, state, stars, type FROM businessReviews")
val reviewDF = spark.sql("SELECT business_id, review_id, cool, date, funny, useful, type FROM reviews")

val innerJoinDF = businessDF.join(reviewDF, businessDF("business_id") === reviewDF("business_id"), "inner").limit(100)
innerJoinDF.show

val rightOuterJoinDF = businessDF.join(reviewDF, businessDF("business_id") === reviewDF("business_id"), "right_outer").limit(100)
rightOuterJoinDF.show

// ============= End Show some JOIN operations between business and review data ====================



