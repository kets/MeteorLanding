//
//  MeteorLandingDataManager.swift
//  MeteorLandingApp
//
//  Created by Ketaki Borkar on 5/20/15.
//  Copyright (c) 2015 MIL. All rights reserved.
//

import Foundation
import Alamofire

class MeteorLandingDataManager : NSObject {
    
    var meteorCallback : ((Bool, [MeteorLanding]!) ->())!
    var meteorLandingArray : [MeteorLanding]!
    
    class var sharedInstance : MeteorLandingDataManager {
        
        struct Static {
            static var instance : MeteorLandingDataManager?
            static var token : dispatch_once_t = 0
        }
        
        // Singleton is thread safe and will only create once
        dispatch_once(&Static.token){
            Static.instance = MeteorLandingDataManager()
        }
        
        return Static.instance!
    }
    
    func loadMeteordata (callback: ((Bool, [MeteorLanding]!) -> ())!) {
    //func loadMeteorData () {
       self.meteorCallback = callback
        //var baseURL = "http://localhost:9200/meteors/_search?q=recclass:L6"
//        var baseURL = "http://localhost:9200/test/_search?q="
        //var baseURL = "http://localhost:9200/test/_search?q=year:[%221880%22+TO+%222000%22]&pretty=true?size=1000"
        var baseURL = "http://127.0.0.1:9200/test/_search/?size=1000&pretty=1"
        //var baseURL = "http://localhost:9200/test/_search"
//        var query = "recclass:L6"
        
//        let escapedQuery = query.stringByAddingPercentEncodingWithAllowedCharacters(NSCharacterSet.URLHostAllowedCharacterSet())!
        
       // Alamofire.request(.GET, baseURL).responseJSON{(_,_,JSON,_) in
        Alamofire.request(.GET, baseURL )
         .response {(request, response, data, error) in
            if (error != nil){
                println(response)
                println(error)
            } else {
                let jsonData = data as! NSData
                var jsonResult: NSDictionary = NSJSONSerialization.JSONObjectWithData(jsonData, options: NSJSONReadingOptions.MutableContainers, error: nil) as! NSDictionary
                //println(jsonResult)
                self.parseMeteorResponse(jsonResult)
                self.meteorCallback(true, self.meteorLandingArray)
            }
            
        }
    }
    
    func parseMeteorResponse(meteorResponse: NSDictionary) -> [MeteorLanding] {
        
        if (meteorLandingArray == nil){
            meteorLandingArray = [MeteorLanding]()
        } else {
            meteorLandingArray.removeAll()
        }
        
        if let hitsDictionary = meteorResponse["hits"] as? NSDictionary{
            
            if let hitsArray = hitsDictionary["hits"] as? NSArray{
                for meteor in hitsArray {
                    if let meteorDict = meteor as? NSDictionary {
                        if let meteorLandingDict = meteorDict["_source"] as? NSDictionary{
                            if let meteorLandingObject = MeteorLanding(dictionary: meteorLandingDict) {
                                //println(meteorLandingObject.dictionary)
                                meteorLandingArray.append(meteorLandingObject)
                            }
                        }
                       
                    }
                }
            }
        }
        println("Size: \(meteorLandingArray.count)")
        
        
        return meteorLandingArray
    }

    
}