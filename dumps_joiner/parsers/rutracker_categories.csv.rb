require 'csv'
require 'time'
require_relative 'storage'

# ruby rutracker_categories.csv.rb ~/Documents/data/rutracker.db rutracker.org_db2/rutracker-torrents

db_path = ARGV[0] or raise "db_path"
data_dir = ARGV[1] or raise "data_dir"
storage = Storage.new(db_path)

Dir.glob("#{data_dir}/201*").sort.each do |path|
  source = path[-8..-1]+".csv"
  print "\n#{source}\n"
  CSV.foreach("#{path}/category_info.csv", col_sep: ";") do |category_id, name, fname|
    #p id, name, fname
    storage.save_category(id: category_id.to_i, name: name)
    CSV.foreach("#{path}/#{fname}", col_sep: ";") do |row|
      forum_id, forum_name, id, hash, name, size, created_at = row
      #p [forum_id, forum_name, id, hash, name, size, created_at]
      storage.save({
        id: id.to_i,
        title: name,
        size: size.to_i,
        forum: {id: forum_id.to_i, category_id: category_id.to_i, name: forum_name},
        created_at: created_at && Time.parse(created_at+" +03:00"),
        magnet: "magnet:?xt=urn:btih:" + hash,
        source: source,
      })
    end
  end
end

storage.stop
