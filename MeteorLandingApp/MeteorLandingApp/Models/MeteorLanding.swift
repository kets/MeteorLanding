//
//  MeteorLanding.swift
//  MeteorLandingApp
//
//  Created by Ketaki Borkar on 5/20/15.
//  Copyright (c) 2015 MIL. All rights reserved.
//

import Foundation
import MapKit

class MeteorLanding : JsonObject, MKAnnotation {
    
    var name: String!
    var nametype: String!
    var recclass: String!
    var mass: Double!
    var fall: String!
    var year: String!
    var id: Int = 0
    var reclat: Double = 0.0
    var reclong: Double = 0.0
    
//    init (name : String, nametype : String, recclass : String, mass : String, fall : String, year : String, id : Int, reclat : Double, reclong : Double ) {
//        self.name = name
//        self.nametype = nametype
//        self.recclass = recclass
//        self.mass = mass
//        self.fall = fall
//        self.year = year
//        self.id = id
//        self.reclat = reclat
//        self.reclong = reclong
//        
//    }
    
    var subtitle : String {
        return self.year
    }
    
    var title : String {
        return self.name
    }
    
    var coordinate : CLLocationCoordinate2D {
        return CLLocationCoordinate2D (latitude: self.reclat, longitude: self.reclong)
    }
    
    func pinColor() -> MKPinAnnotationColor  {
        switch recclass {
        case "EH4":
            return .Red
        case "L6":
            return .Purple
        default:
            return .Green
        }
    }
    
}
class CustomPointAnnotation: MKPointAnnotation {
    var imageName: String!
}
