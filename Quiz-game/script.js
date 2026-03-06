const quizData = [
    {
        question: "What is the capital of France?",
        answers: ["London", "Berlin", "Paris", "Madrid"],
        correct: 2
    },
    {
        question: "Which planet is known as the Red Planet?",
        answers: ["Venus", "Mars", "Jupiter", "Saturn"],
        correct: 1
    },
    {
        question: "Who painted the Mona Lisa?",
        answers: ["Vincent van Gogh", "Pablo Picasso", "Leonardo da Vinci", "Michelangelo"],
        correct: 2
    },
    {
        question: "What is the largest ocean on Earth?",
        answers: ["Atlantic Ocean", "Indian Ocean", "Arctic Ocean", "Pacific Ocean"],
        correct: 3
    },
    {
        question: "In which year did World War II end?",
        answers: ["1943", "1944", "1945", "1946"],
        correct: 2
    },
    {
        question: "What is the smallest country in the world?",
        answers: ["Monaco", "Vatican City", "San Marino", "Liechtenstein"],
        correct: 1
    },
    {
        question: "Who wrote 'Romeo and Juliet'?",
        answers: ["Charles Dickens", "William Shakespeare", "Jane Austen", "Mark Twain"],
        correct: 1
    },
    {
        question: "What is the chemical symbol for gold?",
        answers: ["Go", "Gd", "Au", "Ag"],
        correct: 2
    },
    {
        question: "How many continents are there?",
        answers: ["5", "6", "7", "8"],
        correct: 2
    },
    {
        question: "What is the tallest mountain in the world?",
        answers: ["K2", "Kangchenjunga", "Mount Everest", "Lhotse"],
        correct: 2
    },
    {
        question: "Which country is home to the kangaroo?",
        answers: ["New Zealand", "Australia", "South Africa", "Brazil"],
        correct: 1
    },
    {
        question: "What is the largest organ in the human body?",
        answers: ["Heart", "Brain", "Liver", "Skin"],
        correct: 3
    },
    {
        question: "Who was the first person to walk on the moon?",
        answers: ["Buzz Aldrin", "Neil Armstrong", "Yuri Gagarin", "John Glenn"],
        correct: 1
    },
    {
        question: "What is the currency of Japan?",
        answers: ["Yuan", "Won", "Yen", "Ringgit"],
        correct: 2
    },
    {
        question: "How many sides does a hexagon have?",
        answers: ["5", "6", "7", "8"],
        correct: 1
    },
    {
        question: "What is the speed of light?",
        answers: ["300,000 km/s", "150,000 km/s", "450,000 km/s", "600,000 km/s"],
        correct: 0
    },
    {
        question: "Who developed the theory of relativity?",
        answers: ["Isaac Newton", "Albert Einstein", "Stephen Hawking", "Niels Bohr"],
        correct: 1
    },
    {
        question: "What is the longest river in the world?",
        answers: ["Amazon River", "Nile River", "Yangtze River", "Mississippi River"],
        correct: 1
    },
    {
        question: "In which city is the Eiffel Tower located?",
        answers: ["London", "Rome", "Paris", "Berlin"],
        correct: 2
    },
    {
        question: "What is the hardest natural substance on Earth?",
        answers: ["Gold", "Iron", "Diamond", "Platinum"],
        correct: 2
    },
    {
        question: "How many bones are in the adult human body?",
        answers: ["186", "206", "226", "246"],
        correct: 1
    },
    {
        question: "What is the largest planet in our solar system?",
        answers: ["Saturn", "Jupiter", "Neptune", "Uranus"],
        correct: 1
    },
    {
        question: "Who invented the telephone?",
        answers: ["Thomas Edison", "Nikola Tesla", "Alexander Graham Bell", "Benjamin Franklin"],
        correct: 2
    },
    {
        question: "What is the boiling point of water at sea level?",
        answers: ["90°C", "100°C", "110°C", "120°C"],
        correct: 1
    },
    {
        question: "Which language has the most native speakers?",
        answers: ["English", "Spanish", "Mandarin Chinese", "Hindi"],
        correct: 2
    }
];

let currentQuestion = 0;
let score = 0;
let selectedAnswer = null;
let userAnswers = [];
let totalTime = 0;
let questionTime = 45;
let totalTimerInterval = null;
let questionTimerInterval = null;
let quizStartTime = null;
let shuffledQuestions = [];

function shuffleArray(array) {
    const shuffled = [...array];
    for (let i = shuffled.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
    }
    return shuffled;
}

function startQuiz() {
    document.getElementById('startScreen').style.display = 'none';
    document.getElementById('quizScreen').style.display = 'block';
    
    shuffledQuestions = shuffleArray(quizData);
    
    currentQuestion = 0;
    score = 0;
    userAnswers = [];
    totalTime = 0;
    quizStartTime = Date.now();
    
    startTotalTimer();
    loadQuestion();
}

function startTotalTimer() {
    totalTimerInterval = setInterval(() => {
        totalTime++;
        const minutes = Math.floor(totalTime / 60);
        const seconds = totalTime % 60;
        document.getElementById('totalTimer').textContent = 
            `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
    }, 1000);
}

function startQuestionTimer() {
    questionTime = 45;
    updateQuestionTimerDisplay();
    
    if (questionTimerInterval) {
        clearInterval(questionTimerInterval);
    }
    
    questionTimerInterval = setInterval(() => {
        questionTime--;
        updateQuestionTimerDisplay();
        
        if (questionTime <= 10) {
            document.querySelector('.question-timer').classList.add('warning');
        }
        
        if (questionTime <= 0) {
            clearInterval(questionTimerInterval);
            autoSubmitAnswer();
        }
    }, 1000);
}

function updateQuestionTimerDisplay() {
    document.getElementById('questionTimer').textContent = `${questionTime}s`;
}

function loadQuestion() {
    const question = shuffledQuestions[currentQuestion];
    document.getElementById('question').textContent = question.question;
    document.getElementById('option1').textContent = question.answers[0];
    document.getElementById('option2').textContent = question.answers[1];
    document.getElementById('option3').textContent = question.answers[2];
    document.getElementById('option4').textContent = question.answers[3];
    
    document.getElementById('currentQuestion').textContent = currentQuestion + 1;
    document.getElementById('totalQuestions').textContent = shuffledQuestions.length;
    
    const progress = ((currentQuestion + 1) / shuffledQuestions.length) * 100;
    document.getElementById('progress').style.width = progress + '%';
    
    if (currentQuestion === shuffledQuestions.length - 1) {
        document.getElementById('nextBtnText').textContent = 'Submit Quiz';
    } else {
        document.getElementById('nextBtnText').textContent = 'Next Question';
    }
    
    if (currentQuestion > 0) {
        document.getElementById('backBtn').style.display = 'flex';
    } else {
        document.getElementById('backBtn').style.display = 'none';
    }
    
    resetButtons();
    
    if (userAnswers[currentQuestion] !== undefined) {
        selectedAnswer = userAnswers[currentQuestion].selected;
        if (selectedAnswer !== null && selectedAnswer !== -1) {
            const buttons = document.querySelectorAll('.btn');
            buttons[selectedAnswer].classList.add('selected');
            document.getElementById('nextBtn').disabled = false;
        } else {
            selectedAnswer = null;
            document.getElementById('nextBtn').disabled = true;
        }
    } else {
        selectedAnswer = null;
        document.getElementById('nextBtn').disabled = true;
    }
    
    document.querySelector('.question-timer').classList.remove('warning');
    
    if (questionTimerInterval) {
        clearInterval(questionTimerInterval);
    }
    startQuestionTimer();
}

function selectAnswer(answerIndex) {
    selectedAnswer = answerIndex;
    
    const buttons = document.querySelectorAll('.btn');
    buttons.forEach((btn, index) => {
        btn.classList.remove('selected');
        if (index === answerIndex) {
            btn.classList.add('selected');
        }
    });
    
    document.getElementById('nextBtn').disabled = false;
}

function autoSubmitAnswer() {
    if (selectedAnswer === null) {
        selectedAnswer = -1;
    }
    nextQuestion();
}

function previousQuestion() {
    if (currentQuestion > 0) {
        if (questionTimerInterval) {
            clearInterval(questionTimerInterval);
        }
        currentQuestion--;
        loadQuestion();
    }
}

function nextQuestion() {
    if (questionTimerInterval) {
        clearInterval(questionTimerInterval);
    }
    
    if (userAnswers[currentQuestion] !== undefined) {
        userAnswers[currentQuestion] = {
            question: shuffledQuestions[currentQuestion].question,
            answers: shuffledQuestions[currentQuestion].answers,
            selected: selectedAnswer,
            correct: shuffledQuestions[currentQuestion].correct
        };
    } else {
        userAnswers.push({
            question: shuffledQuestions[currentQuestion].question,
            answers: shuffledQuestions[currentQuestion].answers,
            selected: selectedAnswer,
            correct: shuffledQuestions[currentQuestion].correct
        });
    }
    
    currentQuestion++;
    
    if (currentQuestion < shuffledQuestions.length) {
        loadQuestion();
    } else {
        showResults();
    }
}

function resetButtons() {
    const buttons = document.querySelectorAll('.btn');
    buttons.forEach(btn => {
        btn.classList.remove('selected');
        btn.disabled = false;
    });
}

function showResults() {
    if (totalTimerInterval) {
        clearInterval(totalTimerInterval);
    }
    if (questionTimerInterval) {
        clearInterval(questionTimerInterval);
    }
    
    score = 0;
    userAnswers.forEach(item => {
        if (item.selected === item.correct) {
            score++;
        }
    });
    
    document.getElementById('quizScreen').style.display = 'none';
    document.getElementById('resultScreen').style.display = 'block';
    
    const minutes = Math.floor(totalTime / 60);
    const seconds = totalTime % 60;
    document.getElementById('finalTime').textContent = 
        `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
    
    const percentage = Math.round((score / shuffledQuestions.length) * 100);
    
    document.getElementById('scoreNumber').textContent = score;
    document.getElementById('correctAnswers').textContent = score;
    document.getElementById('incorrectAnswers').textContent = shuffledQuestions.length - score;
    document.getElementById('percentageScore').textContent = percentage + '%';
    
    const circumference = 2 * Math.PI * 54;
    const offset = circumference - (percentage / 100) * circumference;
    document.getElementById('scoreRing').style.strokeDashoffset = offset;
    
    let badgeText = '';
    let badgeColor = '';
    if (percentage >= 90) {
        badgeText = 'Outstanding! 🏆';
        badgeColor = 'linear-gradient(135deg, #fbbf24 0%, #f59e0b 100%)';
    } else if (percentage >= 75) {
        badgeText = 'Excellent Work! 🌟';
        badgeColor = 'linear-gradient(135deg, #10b981 0%, #059669 100%)';
    } else if (percentage >= 60) {
        badgeText = 'Good Job! 👍';
        badgeColor = 'linear-gradient(135deg, #3b82f6 0%, #2563eb 100%)';
    } else if (percentage >= 40) {
        badgeText = 'Keep Practicing! 💪';
        badgeColor = 'linear-gradient(135deg, #f59e0b 0%, #d97706 100%)';
    } else {
        badgeText = 'Try Again! 📚';
        badgeColor = 'linear-gradient(135deg, #ef4444 0%, #dc2626 100%)';
    }
    
    const badge = document.getElementById('performanceBadge');
    badge.textContent = badgeText;
    badge.style.background = badgeColor;
    
    displayAnswerReview();
}

function displayAnswerReview() {
    const reviewList = document.getElementById('reviewList');
    reviewList.innerHTML = '';
    
    const correctQuestions = [];
    const incorrectQuestions = [];
    
    userAnswers.forEach((item, index) => {
        const isCorrect = item.selected === item.correct;
        const questionData = { ...item, index: index + 1 };
        
        if (isCorrect) {
            correctQuestions.push(questionData);
        } else {
            incorrectQuestions.push(questionData);
        }
    });
    
    if (correctQuestions.length > 0) {
        const correctSection = document.createElement('div');
        correctSection.className = 'review-section';
        correctSection.innerHTML = `
            <h4 class="section-title correct-title">✓ Correct Answers (${correctQuestions.length})</h4>
        `;
        
        correctQuestions.forEach((item) => {
            const reviewItem = document.createElement('div');
            reviewItem.className = 'review-item correct';
            
            let selectedAnswerText = item.selected === -1 ? 'No answer' : item.answers[item.selected];
            
            reviewItem.innerHTML = `
                <div class="review-question">Q${item.index}. ${item.question}</div>
                <div class="review-answer">
                    <span class="review-label">Your answer:</span>
                    <span class="review-text correct-ans">${selectedAnswerText}</span>
                </div>
            `;
            
            correctSection.appendChild(reviewItem);
        });
        
        reviewList.appendChild(correctSection);
    }
    
    if (incorrectQuestions.length > 0) {
        const incorrectSection = document.createElement('div');
        incorrectSection.className = 'review-section';
        incorrectSection.innerHTML = `
            <h4 class="section-title incorrect-title">✗ Incorrect Answers (${incorrectQuestions.length})</h4>
        `;
        
        incorrectQuestions.forEach((item) => {
            const reviewItem = document.createElement('div');
            reviewItem.className = 'review-item incorrect';
            
            let selectedAnswerText = item.selected === -1 ? 'No answer' : item.answers[item.selected];
            
            reviewItem.innerHTML = `
                <div class="review-question">Q${item.index}. ${item.question}</div>
                <div class="review-answer">
                    <span class="review-label">Your answer:</span>
                    <span class="review-text wrong-ans">${selectedAnswerText}</span>
                </div>
                <div class="review-answer">
                    <span class="review-label">Correct answer:</span>
                    <span class="review-text correct-ans">${item.answers[item.correct]}</span>
                </div>
            `;
            
            incorrectSection.appendChild(reviewItem);
        });
        
        reviewList.appendChild(incorrectSection);
    }
}

function restartQuiz() {
    if (totalTimerInterval) {
        clearInterval(totalTimerInterval);
    }
    if (questionTimerInterval) {
        clearInterval(questionTimerInterval);
    }
    
    currentQuestion = 0;
    score = 0;
    selectedAnswer = null;
    userAnswers = [];
    totalTime = 0;
    questionTime = 45;
    
    document.getElementById('totalTimer').textContent = '00:00';
    document.getElementById('questionTimer').textContent = '45s';
    document.getElementById('progress').style.width = '0%';
    
    document.getElementById('resultScreen').style.display = 'none';
    document.getElementById('startScreen').style.display = 'block';
}