# Hotel Booking Data Analysis
Small Data Analysis project using Spark and Python looking at hotel booking data.

[Data Source](https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand)

## Datasets
### byCountry
* country
* year: arrival_date_year
* month: arrival_date_month
* bookings: COUNT(*)
* stays: COUNT(All records where is_canceled = 0)
* cancellations: COUNT(All records where is_canceled = 1)
* total_nights_stayed: SUM(stays_in_weekend_nights + stays_in_week_nights)
* total_weekend_nights_stayed: SUM(stays_in_weekend_nights)
* total_week_nights_stayed: SUM(stays_in_week_nights)
* average_lead_time
* average_booking_changes
* no_deposit: count of all bookings where no deposit was placed
* non_refund: count of all bookings where a non-refundable deposit was placed
* refundable: count of all bookings where a refundable deposit was placed
* bb: count of all bookings with a BB meal plan
* hb: count of all bookings with a HB meal plan
* fb: count of all bookings with a FB meal plan
* no_meal: count of all bookings with no meal plan (SC or Undefined)
* parking_required: SUM(required_car_parking_spaces)
* special_requests: SUM(total_of_special_requests)
* check_out: count of all bookings where the customer(s) checked out
* cancelled: count of all bookings where the customer(s) cancelled
* no_show: count of all bookings where the customer(s) no-showed
* adr: average adr
* online: count of all bookings booked with an online travel agent
* offline: count of all bookings booked with an offline travel agent or tour operator
* groups_ms: count of all bookings booked with groups
* direct_ms: count of all bookings intended to be booked directly
* corporate_ms: count of all bookings intended to be booked through an employer
* ta_to: count of all bookings booked through a travel agency or tour operator
* direct_ms: count of all bookings booked directly
* corporate_ms: count of all bookings booked through an employer
* gds: count of all bookings booked through gds
* repeated_guests: count of all bookings where the guest was a repeated guest
* previous_cancellations: sum of all bookings cancelled prior to the current booking
* previous_bookings: sum of all bookings where the customer didn't cancel prior to the current booking
* transients: COUNT(All records where customer_type = 'Transient')
* transient_parties: COUNT(All records where customer_type = 'Transient-Party')
* contracts: COUNT(All records where customer_type = 'Contract')
* groups: COUNT(All records where customer_type = 'Group')
* city_hotels: COUNT(All records where hotel = 'City Hotel')
* resort_hotels: COUNT(All records where hotel = 'Resort Hotel')
* adults: SUM(adults)
* children: SUM(children)
* babies: SUM(babies)
* guests: SUM(adults + children + babies)
### byCustomer
* customer_type
* year: arrival_date_year
* month: arrival_date_month
* bookings: COUNT(*)
* stays: COUNT(All records where is_canceled = 0)
* cancellations: COUNT(All records where is_canceled = 1)
* total_nights_stayed: SUM(stays_in_weekend_nights + stays_in_week_nights)
* total_weekend_nights_stayed: SUM(stays_in_weekend_nights)
* total_week_nights_stayed: SUM(stays_in_week_nights)
* average_lead_time
* average_booking_changes
* no_deposit: count of all bookings where no deposit was placed
* non_refund: count of all bookings where a non-refundable deposit was placed
* refundable: count of all bookings where a refundable deposit was placed
* bb: count of all bookings with a BB meal plan
* hb: count of all bookings with a HB meal plan
* fb: count of all bookings with a FB meal plan
* no_meal: count of all bookings with no meal plan (SC or Undefined)
* parking_required: SUM(required_car_parking_spaces)
* special_requests: SUM(total_of_special_requests)
* check_out: count of all bookings where the customer(s) checked out
* cancelled: count of all bookings where the customer(s) cancelled
* no_show: count of all bookings where the customer(s) no-showed
* adr: average adr
* online: count of all bookings booked with an online travel agent
* offline: count of all bookings booked with an offline travel agent or tour operator
* groups_ms: count of all bookings booked with groups
* direct_ms: count of all bookings intended to be booked directly
* corporate_ms: count of all bookings intended to be booked through an employer
* ta_to: count of all bookings booked through a travel agency or tour operator
* direct_ms: count of all bookings booked directly
* corporate_ms: count of all bookings booked through an employer
* gds: count of all bookings booked through gds
* repeated_guests: count of all bookings where the guest was a repeated guest
* previous_cancellations: sum of all bookings cancelled prior to the current booking
* previous_bookings: sum of all bookings where the customer didn't cancel prior to the current booking
* city_hotels: COUNT(All records where hotel = 'City Hotel')
* resort_hotels: COUNT(All records where hotel = 'Resort Hotel')
* adults: SUM(adults)
* children: SUM(children)
* babies: SUM(babies)
* guests: SUM(adults + children + babies)
### byHotel
* hotel
* year: arrival_date_year
* month: arrival_date_month
* bookings: COUNT(*)
* stays: COUNT(All records where is_canceled = 0)
* cancellations: COUNT(All records where is_canceled = 1)
* total_nights_stayed: SUM(stays_in_weekend_nights + stays_in_week_nights)
* total_weekend_nights_stayed: SUM(stays_in_weekend_nights)
* total_week_nights_stayed: SUM(stays_in_week_nights)
* average_lead_time
* average_booking_changes
* no_deposit: count of all bookings where no deposit was placed
* non_refund: count of all bookings where a non-refundable deposit was placed
* refundable: count of all bookings where a refundable deposit was placed
* bb: count of all bookings with a BB meal plan
* hb: count of all bookings with a HB meal plan
* fb: count of all bookings with a FB meal plan
* no_meal: count of all bookings with no meal plan (SC or Undefined)
* parking_required: SUM(required_car_parking_spaces)
* special_requests: SUM(total_of_special_requests)
* check_out: count of all bookings where the customer(s) checked out
* cancelled: count of all bookings where the customer(s) cancelled
* no_show: count of all bookings where the customer(s) no-showed
* adr: average adr
* online: count of all bookings booked with an online travel agent
* offline: count of all bookings booked with an offline travel agent or tour operator
* groups_ms: count of all bookings booked with groups
* direct_ms: count of all bookings intended to be booked directly
* corporate_ms: count of all bookings intended to be booked through an employer
* ta_to: count of all bookings booked through a travel agency or tour operator
* direct_ms: count of all bookings booked directly
* corporate_ms: count of all bookings booked through an employer
* gds: count of all bookings booked through gds
* repeated_guests: count of all bookings where the guest was a repeated guest
* previous_cancellations: sum of all bookings cancelled prior to the current booking
* previous_bookings: sum of all bookings where the customer didn't cancel prior to the current booking
* transients: COUNT(All records where customer_type = 'Transient')
* transient_parties: COUNT(All records where customer_type = 'Transient-Party')
* contracts: COUNT(All records where customer_type = 'Contract')
* groups: COUNT(All records where customer_type = 'Group')
* adults: SUM(adults)
* children: SUM(children)
* babies: SUM(babies)
* guests: SUM(adults + children + babies)
### byAgent
* agent
* year: arrival_date_year
* month: arrival_date_month
* bookings: COUNT(*)
* stays: COUNT(All records where is_canceled = 0)
* cancellations: COUNT(All records where is_canceled = 1)
* total_nights_stayed: SUM(stays_in_weekend_nights + stays_in_week_nights)
* total_weekend_nights_stayed: SUM(stays_in_weekend_nights)
* total_week_nights_stayed: SUM(stays_in_week_nights)
* average_lead_time
* average_booking_changes
* no_deposit: count of all bookings where no deposit was placed
* non_refund: count of all bookings where a non-refundable deposit was placed
* refundable: count of all bookings where a refundable deposit was placed
* bb: count of all bookings with a BB meal plan
* hb: count of all bookings with a HB meal plan
* fb: count of all bookings with a FB meal plan
* no_meal: count of all bookings with no meal plan (SC or Undefined)
* parking_required: SUM(required_car_parking_spaces)
* special_requests: SUM(total_of_special_requests)
* check_out: count of all bookings where the customer(s) checked out
* cancelled: count of all bookings where the customer(s) cancelled
* no_show: count of all bookings where the customer(s) no-showed
* adr: average adr
* online: count of all bookings booked with an online travel agent
* offline: count of all bookings booked with an offline travel agent or tour operator
* groups_ms: count of all bookings booked with groups
* direct_ms: count of all bookings intended to be booked directly
* corporate_ms: count of all bookings intended to be booked through an employer
* ta_to: count of all bookings booked through a travel agency or tour operator
* direct_ms: count of all bookings booked directly
* corporate_ms: count of all bookings booked through an employer
* gds: count of all bookings booked through gds
* repeated_guests: count of all bookings where the guest was a repeated guest
* previous_cancellations: sum of all bookings cancelled prior to the current booking
* previous_bookings: sum of all bookings where the customer didn't cancel prior to the current booking
* transients: COUNT(All records where customer_type = 'Transient')
* transient_parties: COUNT(All records where customer_type = 'Transient-Party')
* contracts: COUNT(All records where customer_type = 'Contract')
* groups: COUNT(All records where customer_type = 'Group')
* city_hotels: COUNT(All records where hotel = 'City Hotel')
* resort_hotels: COUNT(All records where hotel = 'Resort Hotel')
* adults: SUM(adults)
* children: SUM(children)
* babies: SUM(babies)
* guests: SUM(adults + children + babies)
### byCustomer
* customer
* year: arrival_date_year
* month: arrival_date_month
* bookings: COUNT(*)
* stays: COUNT(All records where is_canceled = 0)
* cancellations: COUNT(All records where is_canceled = 1)
* total_nights_stayed: SUM(stays_in_weekend_nights + stays_in_week_nights)
* total_weekend_nights_stayed: SUM(stays_in_weekend_nights)
* total_week_nights_stayed: SUM(stays_in_week_nights)
* average_lead_time
* average_booking_changes
* no_deposit: count of all bookings where no deposit was placed
* non_refund: count of all bookings where a non-refundable deposit was placed
* refundable: count of all bookings where a refundable deposit was placed
* bb: count of all bookings with a BB meal plan
* hb: count of all bookings with a HB meal plan
* fb: count of all bookings with a FB meal plan
* no_meal: count of all bookings with no meal plan (SC or Undefined)
* parking_required: SUM(required_car_parking_spaces)
* special_requests: SUM(total_of_special_requests)
* check_out: count of all bookings where the customer(s) checked out
* cancelled: count of all bookings where the customer(s) cancelled
* no_show: count of all bookings where the customer(s) no-showed
* adr: average adr
* online: count of all bookings booked with an online travel agent
* offline: count of all bookings booked with an offline travel agent or tour operator
* groups_ms: count of all bookings booked with groups
* direct_ms: count of all bookings intended to be booked directly
* corporate_ms: count of all bookings intended to be booked through an employer
* ta_to: count of all bookings booked through a travel agency or tour operator
* direct_ms: count of all bookings booked directly
* corporate_ms: count of all bookings booked through an employer
* gds: count of all bookings booked through gds
* repeated_guests: count of all bookings where the guest was a repeated guest
* previous_cancellations: sum of all bookings cancelled prior to the current booking
* previous_bookings: sum of all bookings where the customer didn't cancel prior to the current booking
* transients: COUNT(All records where customer_type = 'Transient')
* transient_parties: COUNT(All records where customer_type = 'Transient-Party')
* contracts: COUNT(All records where customer_type = 'Contract')
* groups: COUNT(All records where customer_type = 'Group')
* city_hotels: COUNT(All records where hotel = 'City Hotel')
* resort_hotels: COUNT(All records where hotel = 'Resort Hotel')
* adults: SUM(adults)
* children: SUM(children)
* babies: SUM(babies)
* guests: SUM(adults + children + babies)