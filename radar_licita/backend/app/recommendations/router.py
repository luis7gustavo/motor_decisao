from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import get_db
from app.recommendations.schemas import RecommendationSearchRequest, RecommendationSearchResponse
from app.recommendations.service import search_recommendations

router = APIRouter(prefix="/api/v1/recommendations", tags=["recommendations"])


@router.post("/search", response_model=RecommendationSearchResponse)
def recommendations_search(payload: RecommendationSearchRequest, db: Session = Depends(get_db)):
    return search_recommendations(db, payload)

