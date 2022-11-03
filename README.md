# Hotel Booking Data Analysis
Small Data Analysis project using Spark and Python looking at hotel booking data.

[Data Source](https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand)

## Datasets
### byCountry
* country
* arrival_date_year
* arrival_date_month
* reservations: COUNT(*)
* stays: COUNT(All records where is_canceled = 0)
* cancellations: COUNT(All records where is_canceled = 1)
* cancellationRate: cancellations / reservations
* parking_required: SUM(required_car_parking_spaces)
* special_requests: SUM(total_of_special_requests)
* city_hotels: COUNT(All records where hotel = 'City Hotel')
* resort_hotels: COUNT(All records where hotel = 'Resort Hotel')
* transients: COUNT(All records where customer_type = 'Transient')
* transient_parties: COUNT(All records where customer_type = 'Transient-Party')
* contracts: COUNT(All records where customer_type = 'Contract')
* groups: COUNT(All records where customer_type = 'Group')
* adults: SUM(adults)
* children: SUM(children)
* babies: SUM(babies)
* guests: SUM(adults + children + babies)
### byCustomer
* customer_type
* arrival_date_year
* arrival_date_month
* reservations: COUNT(*)
* stays: COUNT(All records where is_canceled = 0)
* cancellations: COUNT(All records where is_canceled = 1)
* cancellationRate: cancellations / reservations
* parking_required: SUM(required_car_parking_spaces)
* special_requests: SUM(total_of_special_requests)
* city_hotels: COUNT(All records where hotel = 'City Hotel')
* resort_hotels: COUNT(All records where hotel = 'Resort Hotel')
* adults: SUM(adults)
* children: SUM(children)
* babies: SUM(babies)
* guests: SUM(adults + children + babies)
### byHotel
* hotel
* arrival_date_year
* arrival_date_month
* reservations: COUNT(*)
* stays: COUNT(All records where is_canceled = 0)
* cancellations: COUNT(All records where is_canceled = 1)
* cancellationRate: cancellations / reservations
* parking_required: SUM(required_car_parking_spaces)
* special_requests: SUM(total_of_special_requests)
* adults: SUM(adults)
* children: SUM(children)
* babies: SUM(babies)
* guests: SUM(adults + children + babies)