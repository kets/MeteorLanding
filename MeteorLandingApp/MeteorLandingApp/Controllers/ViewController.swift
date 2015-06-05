//
//  ViewController.swift
//  MeteorLandingApp
//
//  Created by Ketaki Borkar on 5/20/15.
//  Copyright (c) 2015 MIL. All rights reserved.
//

import UIKit
import MapKit

class ViewController: UIViewController, MKMapViewDelegate {

    @IBOutlet weak var mapView: MKMapView!
    
    var meteorDataArray : [MeteorLanding]!
    
    let regionRadius: CLLocationDistance = 1000
    func centerMapOnLocation(location: CLLocation) {
        let coordinateRegion = MKCoordinateRegionMakeWithDistance(location.coordinate,
            regionRadius * 2, regionRadius * 2)
        mapView.setRegion(coordinateRegion, animated: true)
    }
    override func viewDidLoad() {
        super.viewDidLoad()
        
        // Do any additional setup after loading the view, typically from a nib.
        //let initialLocation = CLLocation(latitude: 20.06667, longitude: -103.66667)
        //centerMapOnLocation(initialLocation)
        mapView.delegate = self
        
        MeteorLandingDataManager.sharedInstance.loadMeteordata(meteorDataReturned)
        
    }
    
    func meteorDataReturned(success : Bool, meteorDataArray : [MeteorLanding]!){
        if (success) {
            self.meteorDataArray = meteorDataArray
//            for meteors in self.meteorDataArray {
//                mapView.addAnnotation(meteors)
//            }
            //mapView.addAnnotations(self.meteorDataArray)
            mapView.showAnnotations(self.meteorDataArray, animated: true)

            println("mapView size: \(mapView.annotations.count)")
            
        }
        
    }
    
//    func (void)zoomToFitMapAnnotations:(MKMapView*)mapView
//    {
//    if([mapView.annotations count] == 0)
//    return;
//    
//    CLLocationCoordinate2D topLeftCoord;
//    topLeftCoord.latitude = -90;
//    topLeftCoord.longitude = 180;
//    
//    CLLocationCoordinate2D bottomRightCoord;
//    bottomRightCoord.latitude = 90;
//    bottomRightCoord.longitude = -180;
//    
//    for(MapAnnotation* annotation in mapView.annotations)
//    {
//    topLeftCoord.longitude = fmin(topLeftCoord.longitude, annotation.coordinate.longitude);
//    topLeftCoord.latitude = fmax(topLeftCoord.latitude, annotation.coordinate.latitude);
//    
//    bottomRightCoord.longitude = fmax(bottomRightCoord.longitude, annotation.coordinate.longitude);
//    bottomRightCoord.latitude = fmin(bottomRightCoord.latitude, annotation.coordinate.latitude);
//    }
//    
//    MKCoordinateRegion region;
//    region.center.latitude = topLeftCoord.latitude - (topLeftCoord.latitude - bottomRightCoord.latitude) * 0.5;
//    region.center.longitude = topLeftCoord.longitude + (bottomRightCoord.longitude - topLeftCoord.longitude) * 0.5;
//    region.span.latitudeDelta = fabs(topLeftCoord.latitude - bottomRightCoord.latitude) * 1.1; // Add a little extra space on the sides
//    region.span.longitudeDelta = fabs(bottomRightCoord.longitude - topLeftCoord.longitude) * 1.1; // Add a little extra space on the sides
//    
//    region = [mapView regionThatFits:region];
//    [mapView setRegion:region animated:YES];
//    }


}



