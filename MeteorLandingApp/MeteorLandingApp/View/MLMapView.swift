//
//  MLMapView.swift
//  MeteorLandingApp
//
//  Created by Ketaki Borkar on 5/20/15.
//  Copyright (c) 2015 MIL. All rights reserved.
//

import Foundation
import MapKit

extension ViewController : MKMapViewDelegate {
    
    func mapView (mapView : MKMapView!, viewForAnnotation annotation : MKAnnotation!) -> MKAnnotationView! {
        if let annotation = annotation as? MeteorLanding {
            let identifier = "pin"
            var view : MKPinAnnotationView
            if let dequeuedView = mapView.dequeueReusableAnnotationViewWithIdentifier(identifier) as? MKPinAnnotationView {
                dequeuedView.annotation = annotation
                view = dequeuedView
            } else {
                view = MKPinAnnotationView (annotation: annotation, reuseIdentifier: identifier)
                view.canShowCallout = true
                view.calloutOffset = CGPoint(x: -5, y:5)
                view.rightCalloutAccessoryView = UIButton.buttonWithType(.DetailDisclosure) as! UIView
            }
            view.animatesDrop = true
           
            view.pinColor = annotation.pinColor()
            return view
        }
        return nil
    }
    
}