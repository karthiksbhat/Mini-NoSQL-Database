class ApiController < ApplicationController
	protect_from_forgery with: :null_session, if: Proc.new { |c| c.request.format == 'application/json' || c.request.format == 'application/x-www-form-urlencoded'}
	def insert
		#sample api query
		#/api/insert?collection=<collection_name>&values=[attr1:val1,attr2:val2]&primary_keys="key1,key2"&compressed=True|true|false

		collection=params[:collection]
		values=params[:values]
		puts collection
		values[0]='{'
		values[values.length-1]='}'
		# values.gsub! '\"','"'
		# values.gsub! '"','\"'
		# values.gsub! '\",\"','\"\,\"'
		puts values
		primary_keys=params[:primary_keys]
		primary_keys.gsub! '"',''
		puts primary_keys
		compressed=params[:compressed]
		puts compressed

		# call the necessary method from Backend with collection & conditions hash
		# get output and return as json to api client
		if primary_keys!=''
			f=File.open("/home/nitin/Desktop/DATABASE-PROJECT/request_files/temp.txt","w")
			f.puts "send.py"
			f.puts collection
			f.puts "store"
			f.puts values
			f.puts primary_keys
			f.puts compressed
			f.close
			display_output='python /home/nitin/Desktop/DATABASE-PROJECT/send.py temp.txt'
		else
			f=File.open("/home/nitin/Desktop/DATABASE-PROJECT/request_files/temp.txt","w")
			f.puts "send.py"
			f.puts collection
			f.puts "store"
			f.puts values
			f.close
			display_output='python /home/nitin/Desktop/DATABASE-PROJECT/send.py temp.txt'
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
		# values.gsub! '\"','"'
		# values.gsub! '"','\"'
		# values.gsub! '\",\"','\"\,\"'
		puts values

		# call the necessary method from Backend with collection & conditions hash
		# get output and return as json to api client

		f=File.open("/home/nitin/Desktop/DATABASE-PROJECT/request_files/temp.txt","w")
		f.puts "send.py"
		f.puts collection
		f.puts "select"
		f.puts values
		f.close

		display_output='python /home/nitin/Desktop/DATABASE-PROJECT/send.py temp.txt'
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
		# conditions.gsub! '\"','"'
		# conditions.gsub! '"','\"'
		# conditions.gsub! '\",\"','\"\,\"'
		puts conditions


		values[0]='{'
		values[values.length-1]='}'
		# values.gsub! '\"','"'
		# values.gsub! '"','\"'
		# values.gsub! '\",\"','\"\,\"'
		puts values
		

		# call the necessary method from Backend with collection & conditions hash
		# get output and return as json to api client

		f=File.open("/home/nitin/Desktop/DATABASE-PROJECT/request_files/temp.txt","w")
		f.puts "send.py"
		f.puts collection
		f.puts "update"
		f.puts conditions
		f.puts values
		f.close

		display_output='python /home/nitin/Desktop/DATABASE-PROJECT/send.py temp.txt'
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
		# values.gsub! '\"','"'
		# values.gsub! '"','\"'
		# values.gsub! '\",\"','\"\,\"'
		puts values

		# call the necessary method from Backend with collection & conditions hash
		# get output and return as json to api client

		f=File.open("/home/nitin/Desktop/DATABASE-PROJECT/request_files/temp.txt","w")
		f.puts "send.py"
		f.puts collection
		f.puts "delete"
		f.puts values
		f.close

		display_output='python /home/nitin/Desktop/DATABASE-PROJECT/send.py temp.txt'
		return_output = `#{display_output}`
		response_json = {'response'=>return_output}.to_json

		render json: response_json
	end

	def desc
		#sample api query
		#/api/desc?collection=<collection_name>
		collection=params[:collection]
		f=File.open("/home/nitin/Desktop/DATABASE-PROJECT/request_files/temp.txt","w")
		f.puts "send.py"
		f.puts collection
		f.puts "desc"
		f.close
		display_output='python /home/nitin/Desktop/DATABASE-PROJECT/send.py temp.txt'
		puts display_output
		# File.delete("/home/nitin/Desktop/DATABASE-PROJECT/request_files/temp.txt")
		return_output = `#{display_output}`
		puts return_output
		response_json = {'response'=>return_output}.to_json

		render json: response_json
	end

	def drop
		#sample api query
		#/api/drop?collection=<collection_name>
		collection=params[:collection]
		f=File.open("/home/nitin/Desktop/DATABASE-PROJECT/request_files/temp.txt","w")
		f.puts "send.py"
		f.puts collection
		f.puts "drop"
		f.close
		display_output='python /home/nitin/Desktop/DATABASE-PROJECT/send.py temp.txt'
		puts display_output
		return_output = `#{display_output}`
		puts return_output
		response_json = {'response'=>return_output}.to_json

		render json: response_json
	end

end
