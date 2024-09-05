from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient('mongodb://adminUser:securePassword@45.56.127.88/?authSource=admin&tls=false')
result = client['PortalAuction']['IntegratedData'].aggregate([
    {
        '$project': {
            'SoldOn': 'IAAI', 
            'Name': '$Info.Name', 
            'prices': 1, 
            'Images': '$Info.Images', 
            'Stock': {
                '$ifNull': [
                    '$Info.Vehicle Info.№ артикула:', {
                        '$ifNull': [
                            '$Info.Vehicle Info.Nr zapasów:', {
                                '$ifNull': [
                                    '$Info.Vehicle Info.Nro. de existencia:', {
                                        '$ifNull': [
                                            '$Info.Vehicle Info.存货编号:', '$Info.Vehicle Info.Stock #:'
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }, 
            'VIN': {
                '$arrayElemAt': [
                    {
                        '$split': [
                            {
                                '$ifNull': [
                                    '$Info.Vehicle Info.VIN (Status):', {
                                        '$ifNull': [
                                            '$Info.Vehicle Info.VIN:', {
                                                '$ifNull': [
                                                    '$Info.Vehicle Info.VIN', ''
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }, ' '
                        ]
                    }, 0
                ]
            }, 
            'Title Code': '$Info.Vehicle Info.Title/Sale Doc:', 
            'Odometer': {
                '$arrayElemAt': [
                    {
                        '$split': [
                            '$Info.Vehicle Info.Odometer:', ' '
                        ]
                    }, 0
                ]
            }, 
            'Primary Damage': '$Info.Vehicle Info.Primary Damage:', 
            'Secondary Damage': '$Info.Vehicle Info.Secondary Damage:', 
            'Cylinders': {
                '$arrayElemAt': [
                    {
                        '$split': [
                            '$Info.Vehicle Description.Cylinders:', ' '
                        ]
                    }, 0
                ]
            }, 
            'Color': '$Info.Vehicle Info.Color', 
            'Engine': {
                '$arrayElemAt': [
                    {
                        '$split': [
                            '$Info.Vehicle Description.Engine:', ' '
                        ]
                    }, 0
                ]
            }, 
            'Start Code': '$Info.Vehicle Info.Start Code:', 
            'Drive': '$Info.Vehicle Description.Drive Line Type:', 
            'Vehicle': '$Info.Vehicle Description.Vehicle:', 
            'Selling Branch': '$Info.Sale Info.Selling Branch:'
        }
    }, {
        '$merge': {
            'into': {
                'db': 'MergedDB', 
                'coll': 'IaaiCopart'
            }, 
            'whenMatched': 'replace', 
            'whenNotMatched': 'insert'
        }
    }
])
