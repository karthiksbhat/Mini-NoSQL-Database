class ApiController < ApplicationController
	def insert
		#sample api query
		#/api/insert?collection=<collection_name>&values=[attr1:val1,attr2:val2]

		collection=params[:collection]
		values=params[:values]
		puts collection
		values[0]='{'
		values[values.length-1]='}'
		values_json=JSON.parse(values)
		#puts values_json["one"]

		# call the necessary method from Backend with collection & values hash
		# get output and return as json to api client

		render json: values_json
	end

	def display
		#sample api query
		#/api/display?collection=<collection_name>&conditions=[attr1:val1,attr2:val2]

		collection=params[:collection]
		conditions=params[:conditions]
		puts collection
		conditions[0]='{'
		conditions[conditions.length-1]='}'
		conditions_json=JSON.parse(conditions)

		# call the necessary method from Backend with collection & conditions hash
		# get output and return as json to api client

		render json: conditions_json
	end

	def modify
		#sample api query
		#/api/modify?collection=<collection_name>&conditions=[attr1:val1,attr2:val2]&values=[attr1:val1,attr2:val2]

		collection=params[:collection]
		conditions=params[:conditions]
		values=params[:values]
		puts collection
		conditions[0]='{'
		conditions[conditions.length-1]='}'
		conditions_json=JSON.parse(conditions)
		values[0]='{'
		values[values.length-1]='}'
		values_json=JSON.parse(values)

		# call the necessary method from Backend with collection & values & conditions hash
		# get output and return as json to api client

		render json: values_json
	end

	def delete
		#sample api query
		#/api/delete?collection=<collection_name>&conditions=[attr1:val1,attr2:val2]

		collection=params[:collection]
		conditions=params[:conditions]
		puts collection
		conditions[0]='{'
		conditions[conditions.length-1]='}'
		conditions_json=JSON.parse(conditions)

		# call the necessary method from Backend with collection & conditions hash
		# get output and return as json to api client

		render json: conditions_json
	end

end
