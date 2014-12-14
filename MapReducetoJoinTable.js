business_map = function() {
    emit(this.business_id, {categories: this.categories, reviewdata: " "});
}

review_map=function(){
    emit(this.business_id,{reviewdata:this.text, categories: " "});
}

r = function(key, values) {
    var result = {reviewdata: " ", categories:" "};
    values.forEach(function(value) {
		result.reviewdata += (value.reviewdata !== null) ? value.reviewdata :" ";
		if ( result.categories === " " || value.categories!==null ) {
			result.categories= value.categories;
		}
    });
    
    return result;
}

res = db.business.mapReduce(business_map, r, {out: {reduce: 'joined'}})

res = db.reviewData.mapReduce(review_map, r, {out: {reduce: 'joined'} })

var cnt=db.joined.find().count();
var myCursor=db.joined.find()
for(var i=1;i<=cnt;i++)
{
	db.BRTableRest.insert(myCursor.next());
	
}