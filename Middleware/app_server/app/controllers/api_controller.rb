class ApiController < ApplicationController
	def insert
		#sample api query
		#/api/insert?collection=<collection_name>&values=[attr1:val1,attr2:val2]&primary_keys="key1,key2"&compressed=True|true|false

		collection=params[:collection]
		values=params[:values]
		puts collection
		values[0]='{'
		values[values.length-1]='}'
		values.gsub! '\"','"'
		values.gsub! '"','\"'
		values.gsub! '\",\"','\"\,\"'
		puts values
		primary_keys=params[:primary_keys]
		primary_keys.gsub! '"',''
		puts primary_keys
		compressed=params[:compressed]
		puts compressed

		# call the necessary method from Backend with collection & conditions hash
		# get output and return as json to api client
		if primary_keys!=''
			display_output='python /home/nitin/Desktop/DATABASE-PROJECT/send.py '+collection+' store '+values+' '+primary_keys+' '+compressed
		else
			display_output='python /home/nitin/Desktop/DATABASE-PROJECT/send.py '+collection+' store '+values
		end	
		print display_output
		return_output = `#{display_output}`
		response_json = {'response'=>return_output}.to_json

		render json: response_json
	end

	def display
		#sample api query
		#/api/display?collection=<collection_name>&values=[attr1:val1,attr2:val2]

		collection=params[:collection]
		values=params[:values]
		puts collection
		values[0]='{'
		values[values.length-1]='}'
		values.gsub! '\"','"'
		values.gsub! '"','\"'
		values.gsub! '\",\"','\"\,\"'
		puts values

		# call the necessary method from Backend with collection & conditions hash
		# get output and return as json to api client

		display_output='python /home/nitin/Desktop/DATABASE-PROJECT/send.py '+collection+' select '+values
		return_output = `#{display_output}`
		response_json = {'response'=>return_output}.to_json

		render json: response_json
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
		conditions.gsub! '\"','"'
		conditions.gsub! '"','\"'
		conditions.gsub! '\",\"','\"\,\"'
		puts conditions


		values[0]='{'
		values[values.length-1]='}'
		values.gsub! '\"','"'
		values.gsub! '"','\"'
		values.gsub! '\",\"','\"\,\"'
		puts values
		

		# call the necessary method from Backend with collection & conditions hash
		# get output and return as json to api client

		display_output='python /home/nitin/Desktop/DATABASE-PROJECT/send.py '+collection+' update '+conditions+' '+values
		return_output = `#{display_output}`
		response_json = {'response'=>return_output}.to_json

		render json: response_json
	end

	def delete
		#sample api query
		#/api/delete?collection=<collection_name>&values=[attr1:val1,attr2:val2]

		collection=params[:collection]
		values=params[:values]
		puts collection
		values[0]='{'
		values[values.length-1]='}'
		values.gsub! '\"','"'
		values.gsub! '"','\"'
		values.gsub! '\",\"','\"\,\"'
		puts values

		# call the necessary method from Backend with collection & conditions hash
		# get output and return as json to api client

		display_output='python /home/nitin/Desktop/DATABASE-PROJECT/send.py '+collection+' delete '+values
		return_output = `#{display_output}`
		response_json = {'response'=>return_output}.to_json

		render json: response_json
	end

	def desc
		#sample api query
		#/api/desc?collection=<collection_name>
		collection=params[:collection]

		display_output='python /home/nitin/Desktop/DATABASE-PROJECT/send.py '+collection+' desc'
		return_output = `#{display_output}`
		puts return_output
		response_json = {'response'=>return_output}.to_json

		render json: response_json
	end

end
