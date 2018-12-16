require 'rubygems/package'
require 'zlib'
require 'base32'#gem install --remote base32
require 'time'
require_relative 'storage'

# ruby rutracker_final.txt.rb ~/Documents/data/rutracker.db rutracker.org_db/release/

db_path = ARGV[0] or raise "db_path"
data_dir = ARGV[1] or raise "data_dir"
storage = Storage.new(db_path)

count = 0

Zlib::GzipReader.open("#{data_dir}/final.txt.gz") do |gz|
  begin
    while true
      id, name, size, seeds, leeches, hash_base32, downloads, updated_at = gz.readline.strip.split("\t")
      storage.save({
        id: id.to_i,
        title: name,
        size: size.to_i,
        #seeds: seeds.to_i,
        #leeches: leeches.to_i,
        #downloads: downloads.to_i,
        created_at: Time.parse(updated_at+" +03:00"),
        magnet: "magnet:?xt=urn:btih:" + Base32.decode(hash_base32).unpack('H*').join.upcase,
        source: "final.txt",
      })
    end
  rescue EOFError
  end
end

print "\ndescriptions\n"

Dir.glob("#{data_dir}/*/*.tar.gz").each do |path|
  Zlib::GzipReader.open(path) do |gz|
    Gem::Package::TarReader.new(gz) do |tar|
      tar.each do |entry|
        id = entry.full_name.to_i
        storage.update_description(id, entry.read)
      end
    end
  end
end

storage.stop

