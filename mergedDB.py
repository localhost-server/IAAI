from pymongo import MongoClient
import time

client = MongoClient('mongodb://adminUser:securePassword@45.56.127.88/?authSource=admin&tls=false')
# db=client['MergedDB']
# col=db['IaaiCopart']
# col.delete_many({})
# time.sleep(120)

result = client['PortalAuction']['IntegratedData'].aggregate([
    {
        '$project': {
            'SoldOn': 'IAAI', 
            'Name': '$Info.Name', 
            'Year': {
                '$arrayElemAt': [
                    {
                        '$split': [
                            '$Info.Name', ' '
                        ]
                    }, 0
                ]
            }, 
            'Make': {
                '$let': {
                    'vars': {
                        'secondWord': {
                            '$arrayElemAt': [
                                {
                                    '$split': [
                                        '$Info.Name', ' '
                                    ]
                                }, 1
                            ]
                        }
                    }, 
                    'in': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$eq': [
                                            '$$secondWord', 'ALFA'
                                        ]
                                    }, 
                                    'then': 'ALFA ROMEO'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$$secondWord', 'ASTON'
                                        ]
                                    }, 
                                    'then': 'ASTON MARTIN'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$$secondWord', 'LAND'
                                        ]
                                    }, 
                                    'then': 'LAND ROVER'
                                }, {
                                    'case': {
                                        '$eq': [
                                            '$$secondWord', 'AMERICAN'
                                        ]
                                    }, 
                                    'then': 'AMERICAN MOTORS'
                                }
                            ], 
                            'default': '$$secondWord'
                        }
                    }
                }
            }, 
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
        '$project': {
            'SoldOn': 1, 
            'Name': 1, 
            'Year': 1, 
            'Make': 1, 
            'remaining_string': {
                '$trim': {
                    'input': {
                        '$replaceOne': {
                            'input': '$Name', 
                            'find': {
                                '$concat': [
                                    '$Year', ' ', '$Make'
                                ]
                            }, 
                            'replacement': ''
                        }
                    }
                }
            }, 
            'prices': 1, 
            'Images': 1, 
            'Stock': 1, 
            'VIN': 1, 
            'Title Code': 1, 
            'Odometer': 1, 
            'Primary Damage': 1, 
            'Secondary Damage': 1, 
            'Cylinders': 1, 
            'Color': 1, 
            'Engine': 1, 
            'Start Code': 1, 
            'Drive': 1, 
            'Vehicle': 1, 
            'Selling Branch': 1
        }
    }, {
        '$project': {
            'SoldOn': 1, 
            'Name': 1, 
            'Year': 1, 
            'Make': 1, 
            'Model': {
                '$let': {
                    'vars': {
                        'matchedModel': {
                            '$filter': {
                                'input': [
                                    '718 BOXSTER', 'NEW BEETLE', 'LAND CRUISER', 'MODEL S', 'MODEL X', 'MODEL Y', 'MODEL 3', 'GRAND VITARA', '718 CAYMAN', '3000 GT', 'AMG GT', 'TOWN CAR', 'GRAND WAGONEER', 'SANTA FE', 'SANTA CRUZ', 'CROWN VICTORIA', '458 ITALIA', 'RAM 3500', 'RAM 2500', 'RAM 1500', 'GRAND CARAVAN', 'MONTE CARLO', 'EL CAMINO', 'PARK AVENUE', 'GRAND CHEROKEE', 'RANGE ROVER'
                                ], 
                                'as': 'model', 
                                'cond': {
                                    '$regexMatch': {
                                        'input': '$remaining_string', 
                                        'regex': {
                                            '$concat': [
                                                '\\b', '$$model', '\\b'
                                            ]
                                        }, 
                                        'options': 'i'
                                    }
                                }
                            }
                        }
                    }, 
                    'in': {
                        '$cond': {
                            'if': {
                                '$gt': [
                                    {
                                        '$size': '$$matchedModel'
                                    }, 0
                                ]
                            }, 
                            'then': {
                                '$arrayElemAt': [
                                    '$$matchedModel', 0
                                ]
                            }, 
                            'else': {
                                '$arrayElemAt': [
                                    {
                                        '$split': [
                                            '$remaining_string', ' '
                                        ]
                                    }, 0
                                ]
                            }
                        }
                    }
                }
            }, 
            'prices': 1, 
            'Images': 1, 
            'Stock': 1, 
            'VIN': 1, 
            'Title Code': 1, 
            'Odometer': 1, 
            'Primary Damage': 1, 
            'Secondary Damage': 1, 
            'Cylinders': 1, 
            'Color': 1, 
            'Engine': 1, 
            'Start Code': 1, 
            'Drive': 1, 
            'Vehicle': 1, 
            'Selling Branch': 1
        }
    }, {
        '$project': {
            'SoldOn': 1, 
            'Name': 1, 
            'Year': 1, 
            'Make': 1, 
            'Model': 1, 
            'Model2': {
                '$trim': {
                    'input': {
                        '$replaceOne': {
                            'input': '$Name', 
                            'find': {
                                '$concat': [
                                    '$Year', ' ', '$Make', ' ', '$Model'
                                ]
                            }, 
                            'replacement': ''
                        }
                    }
                }
            }, 
            'prices': 1, 
            'Images': 1, 
            'Stock': 1, 
            'VIN': 1, 
            'Title Code': 1, 
            'Odometer': 1, 
            'Primary Damage': 1, 
            'Secondary Damage': 1, 
            'Cylinders': 1, 
            'Color': 1, 
            'Engine': 1, 
            'Start Code': 1, 
            'Drive': 1, 
            'Vehicle': 1, 
            'Selling Branch': 1
        }
    }, {
        '$project': {
            'SoldOn': 1, 
            'Name': 1, 
            'Year': {
                '$convert': {
                    'input': '$Year', 
                    'to': 'int', 
                    'onError': None, 
                    'onNull': None
                }
            }, 
            'Make': 1, 
            'Model': 1, 
            'Model2': 1, 
            'prices': 1, 
            'Images': 1, 
            'Stock': 1, 
            'VIN': 1, 
            'Title Code': 1, 
            'Odometer': 1, 
            'Primary Damage': 1, 
            'Secondary Damage': 1, 
            'Cylinders': 1, 
            'Color': 1, 
            'Engine': 1, 
            'Start Code': 1, 
            'Drive': 1, 
            'Vehicle': 1, 
            'Selling Branch': 1
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
