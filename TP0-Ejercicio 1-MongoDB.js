db.albums.aggregate([{$group: {_id: "$Year", count: {$sum: 1}}}, {$sort: {count: -1}}]);

db.albums.updateMany({}, [{$set: {score: {$subtract: [501, "$Number"]}}}]);

db.albums.aggregate([{$group: {_id: "$Artist", score: {$sum: "$score"}}}, {$sort: {score: -1}}]);