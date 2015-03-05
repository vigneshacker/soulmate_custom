module Soulmate

  class Matcher < Base

    def matches_for_term(term, options = {})
      options = { :limit => 5, :cache => true, :city => nil }.merge(options)
      
      words = normalize(term).split(' ').reject do |w|
        w.size < Soulmate.min_complete or Soulmate.stop_words.include?(w)
      end.sort

      return [] if words.empty?

      filter_text = "city_id:#{options[:city]}"

      cachekey = "#{cachebase}:" + words.join('|') + ":" + filter_text

      if !options[:cache] || !Soulmate.redis.exists(cachekey) || Soulmate.redis.exists(cachekey) == 0
        interkeys = words.map { |w| "#{base}:#{w}" }
        filter_cache_key = "#{cachebase}:city_id:#{options[:city]}"
        if !Soulmate.redis.exists(filter_cache_key) || Soulmate.redis.exists(filter_cache_key) == 0
          unionkeys = []
          unionkeys << "city_id:#{options[:city]}"
          Soulmate.redis.zunionstore(filter_cache_key, unionkeys)
          Soulmate.redis.expire(filter_cache_key, 10 * 60)
        end
        interkeys << filter_cache_key
        Soulmate.redis.zinterstore(cachekey, interkeys)
        Soulmate.redis.expire(cachekey, 10 * 60) # expire after 10 minutes
      end

      ids = Soulmate.redis.zrevrange(cachekey, 0, options[:limit] - 1)
      if ids.size > 0
        results = Soulmate.redis.hmget(database, *ids)
        results = results.reject{ |r| r.nil? } # handle cached results for ids which have since been deleted
        results.map { |r| MultiJson.decode(r) }
      else
        []
      end
    end
  end
end
