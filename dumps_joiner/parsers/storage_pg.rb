require 'pg' #gem install pg
require 'set'

class Storage
  def initialize(addr_port)
    @queue = SizedQueue.new 5
    Thread.new do
      s = StoragePG.new(addr_port)
      until @queue.closed?
        method, *args = @queue.shift
        s.send(method, *args)
      end
    end
  end

  def save(torrent)
    @queue.push [__method__, torrent]
  end

  def update_description(id, description)
    @queue.push [__method__, id, description]
  end

  def save_category(cat)
    @queue.push [__method__, cat]
  end
  
  def stop
    @queue.push [__method__]
  end
end


class StoragePG
  def initialize(addr_port)
    addr, port = addr_port.split(':')
    @db = PG.connect :dbname => 'rutracker_db', :host => addr, :port => port
    @known_forum_ids = Set.new
    @count = {total: 0, saved: 0, created: 0, updated: 0, skipped: 0}
    @stt = Time.now

    @db.exec <<-SQL
      CREATE TABLE IF NOT EXISTS torrents (
        id INTEGER PRIMARY KEY,
        forum_id INTEGER,
        title TEXT,
        title_old TEXT,
        description TEXT,
        magnet TEXT,
        size BIGINT,
        created_at TIMESTAMPTZ,
        source TEXT
      )
    SQL

    @db.exec <<-SQL
      CREATE TABLE IF NOT EXISTS forums (
        id INTEGER PRIMARY KEY,
        category_id INTEGER,
        name TEXT
      )
    SQL

    @db.exec <<-SQL
      CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY,
        name TEXT
      )
    SQL

    @db.exec "BEGIN"
  end

  def save(torrent)
    data = {
      id:          torrent[:id],
      forum_id:    torrent[:forum] && torrent[:forum][:id],
      title:       torrent[:title],
      title_old:   nil,
      description: torrent[:description],
      magnet:      torrent[:magnet],
      size:        torrent[:size],
      created_at:  torrent[:created_at],
      source:      torrent[:source],
    }

    if false
    res = @db.exec("SELECT * FROM torrents WHERE id = $1", [torrent[:id]])
    row = res.ntuples == 0 ? nil : res[0]
    if row
      data.delete_if{|k,v| v.nil? }
      cmp_keys = data.keys - [:source]
      if data.values_at(*cmp_keys) != row.values_at(*cmp_keys.map(&:to_s))
        data[:title_old] = row["title"] if data[:title] != row["title"]
        update(data)
      else
        @count[:skipped] += 1
      end
    else
      create(data)
    end
    end

    insert_keys = data.keys.join(',')
    insert_places = data.size.times.map{|i| "$#{i+1}" }.join(',')
    update_keys_places = data.keys.each_with_index.map{|k,i| "#{k}=$#{i+1}" unless k==:id or data[k].nil? }.compact.join(',')
    values = data.values

    xmax = @db.exec(%Q[
      INSERT INTO torrents (#{insert_keys}) VALUES (#{insert_places})
      ON CONFLICT (id) DO UPDATE SET #{update_keys_places}
      RETURNING xmax
    ], values)[0]['xmax']
    @count[:saved] += 1
    if xmax == '0'
      @count[:created] += 1
    else
      @count[:updated] += 1
    end

    create_forum(torrent[:forum]) if torrent[:forum]

    after_save
  end
  
  def update_description(id, description)
    @db.exec "UPDATE torrents SET description = $1 WHERE id = $2", [description, id]
    after_save
  end
  
  def save_category(cat)
    @db.exec(%Q[
      INSERT INTO categories (id, name) VALUES ($1, $2)
      ON CONFLICT (id) DO UPDATE SET name = $2
    ], [cat[:id], cat[:name]])
  end
  
  def stop
    @db.exec "COMMIT"
  end
  
  private
  
  def create_forum(forum)
    return if @known_forum_ids.include? forum[:id]
    @known_forum_ids.add(forum[:id])
    @db.exec "INSERT INTO forums (id, category_id, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
             [forum[:id], forum[:category_id], forum[:name]]
  end
  
  def create(data)
    keys = data.keys.join(',')
    places = data.size.times.map{|i| "$#{i+1}"}.join(',')
    @db.exec "INSERT INTO torrents (#{keys}) VALUES (#{places})", data.values
    @count[:saved] += 1
    @count[:created] += 1
  end
  
  def update(data)
    params = data.keys.each_with_index.map{|k,i| "#{k}=$#{i+1}" }.join(',')
    @db.exec "UPDATE torrents SET #{params} WHERE id = $#{data.size+1}", data.values + [data[:id]]
    @count[:saved] += 1
    @count[:updated] += 1
  end
  
  def after_save
    @count[:total] += 1
    print "s:#{@count[:saved]} c:#{@count[:created]} u:#{@count[:updated]} k:#{@count[:skipped]} t:#{@count[:total]} #{(@count[:total] / (Time.now-@stt)).to_i}t/s\r" if @count[:total] % 100 == 0
    if @count[:total] % 10000 == 0
      @db.exec "COMMIT"
      @db.exec "BEGIN"
    end
  end
end
